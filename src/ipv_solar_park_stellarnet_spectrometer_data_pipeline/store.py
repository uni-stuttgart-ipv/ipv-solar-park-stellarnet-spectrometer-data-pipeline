from typing import TYPE_CHECKING, Optional
import os
import posixpath
import logging
import datetime as dt
import tempfile
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import influxdb_client_3 as influx
from . import notify

if TYPE_CHECKING:
    import pandas as pd

AWS_S3_BUCKET_NAME = "solar-park-spectra"
AWS_ACCESS_KEY_ID_ENV_KEY = "SOLAR_PARK_SPECTRA_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY_ENV_KEY = "SOLAR_PARK_SPECTRA_AWS_SECRET_ACCESS_KEY"
AWS_SPECTRA_DATA_FILE_PREFIX = "spectra"
AWS_DATA_FILE_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"
INFLUXDB_HOST = "https://eu-central-1-1.aws.cloud2.influxdata.com"
INFLUXDB_DATABASE = "solar_park_data"
INFLUXDB_TOKEN_ENV_KEY = "SOLAR_PARK_SPECTRA_INFLUXDB_TOKEN"
INFLUXDB_MEASUREMENT_NAME = "solar_spectra"
INFLUXDB_SPECTROMETER_TAG_NAME = "spectrometer"
INFLUXDB_SPECTROMETER_BUCKET_FIELD_NAME = "s3_bucket"
INFLUXDB_SPECTROMETER_FILE_PATH_FIELD_NAME = "s3_object_key"
LOG_FILE = "store.log"

logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_FILE, encoding="utf-8", level=logging.DEBUG)


def store_spectra_in_s3(
    spectra: "pd.DataFrame", timestamp: dt.datetime, spectrometer_id: str
) -> str | None:
    """Upload spectral data into the respective AWS S3 bucket.

    Args:
        spectra (pd.DataFrame): Spectral data to upload.
        timestamp (dt.datetime): Timestamp of the spectra.
        spectrometer_id (str): Id of the spectrometer that took the spectra.

    Returns:
        str | None: S3 object key (object path) of the saved file. `None` if an error occured.

    Raises:
        RuntimeError: Required environment variables are not set.
    """
    access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
    secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)
    if access_key_id is None:
        raise RuntimeError(
            f"environment variable {AWS_ACCESS_KEY_ID_ENV_KEY} is not set"
        )
    if secret_access_key is None:
        raise RuntimeError(
            f"environment variable {AWS_SECRET_ACCESS_KEY_ENV_KEY} is not set"
        )

    s3 = boto3.client(
        "s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key
    )

    timestamp_str = timestamp.strftime(AWS_DATA_FILE_TIMESTAMP_FORMAT)
    filename = f"{timestamp_str}.csv"
    object_key = posixpath.join(AWS_SPECTRA_DATA_FILE_PREFIX, spectrometer_id, filename)

    with tempfile.NamedTemporaryFile(delete_on_close=False) as f:
        spectra.to_csv(f.name, header=False)

        try:
            s3.upload_file(f.name, AWS_S3_BUCKET_NAME, object_key)
        except ClientError as e:
            logger.error(e)
            notify_credentials = notify.get_credentials()
            if notify_credentials is not None:
                msg = f"Failed writing data to AWS S3.\n\n{e}\n\nCheck logs for more details."
                notify.send_error_email(notify_credentials, msg)
            return None

    return object_key


def influx_success(self, data: str):
    logger.info(f"Successfully wrote batch: data: {data}")


def influx_error(
    self,
    data: str,
    exception: influx.InfluxDBError,
):
    """Log influx write error.

    Args:
        data (str): Data trying to be written.
        exception (influx.InfluxDBError): Error that ocurred.
    """
    logger.error(f"Failed writing batch: config: {self}, data: {data} due: {exception}")

    notify_credentials = notify.get_credentials()
    if notify_credentials is not None:
        msg = f"Failed writing data to InfluxDB.\n\n{exception}\n\nCheck logs for more details."
        notify.send_error_email(notify_credentials, msg)


def influx_retry(self, data: str, exception: influx.InfluxDBError):
    logger.debug(
        f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}"
    )


def register_spectra_in_influxdb(
    timestamp: dt.datetime, s3_object_key: str, spectrometer: str
):
    """Add a reference to a spectral data file to the InfluxDB.

    Args:
        timestamp (dt.datetime): Timestamp associated to the measurement.
        s3_object_key (str): S3 object key (object path) to the data file.
        spectrometer (str): Name of the spectrometer.

    Raises:
        RuntimeError: Required environment variables are not set.
    """
    point = (
        influx.Point(INFLUXDB_MEASUREMENT_NAME)
        .time(timestamp, write_precision=influx.WritePrecision.S)
        .tag(INFLUXDB_SPECTROMETER_TAG_NAME, spectrometer)
        .field(INFLUXDB_SPECTROMETER_BUCKET_FIELD_NAME, AWS_S3_BUCKET_NAME)
        .field(INFLUXDB_SPECTROMETER_FILE_PATH_FIELD_NAME, s3_object_key)
    )

    access_token = os.getenv(INFLUXDB_TOKEN_ENV_KEY)
    if access_token is None:
        raise RuntimeError(f"environment variable {INFLUXDB_TOKEN_ENV_KEY} is not set")

    write_options = influx.WriteOptions(
        flush_interval=10_000,
        jitter_interval=2_000,
        retry_interval=5_000,
        max_retries=5,
        max_retry_delay=30_000,
        exponential_base=2,
    )

    options = influx.write_client_options(
        success_callback=influx_success,
        error_callback=influx_error,
        retry_callback=influx_retry,
        write_options=write_options,
    )

    with influx.InfluxDBClient3(
        host=INFLUXDB_HOST,
        token=access_token,
        database=INFLUXDB_DATABASE,
        write_client_options=options,
    ) as client:
        client.write(point)
