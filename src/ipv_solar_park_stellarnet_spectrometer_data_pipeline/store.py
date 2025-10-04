from typing import TYPE_CHECKING
import os
import logging
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import influxdb_client_3 as influx

if TYPE_CHECKING:
    import pandas as pd

AWS_S3_BUCKET_NAME = "solar-park-spectra"
AWS_ACCESS_KEY_ID_ENV_KEY = "SOLAR_PARK_SPECTRA_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY_ENV_KEY = "SOLAR_PARK_SPECTRA_AWS_SECRET_ACCESS_KEY"
INFLUXDB_HOST = ""
INFLUXDB_DATABASE = "solar_park"
INFLUXDB_TOKEN_ENV_KEY = "SOLAR_PARK_SPECTRA_INFLUXDB_TOKEN"
INFLUXDB_MEASUREMENT_NAME = "solar_spectra"
INFLUXDB_SPECTROMETER_TAG_NAME = "spectrometer"
INFLUXDB_SPECTROMETER_FILE_PATH_FIELD_NAME = "data_file_uri"
INFLUXDB_FIELD = "data_file_path"
LOG_FILE = "store.log"

logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_FILE, encoding="utf-8", level=logging.DEBUG)


def store_spectra_in_s3(spectra: pd.DataFrame) -> bool:
    """Upload spectral data into the respective AWS S3 bucket.

    Args:
        spectra (pd.DataFrame): Spectral data to upload.

    Returns:
        bool: If the data was uploaded successfully.
    """
    access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
    secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)

    s3 = boto3.client(
        "s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key
    )

    try:
        s3.upload_file(filename, AWS_S3_BUCKET_NAME, object_name)
    except ClientError as e:
        logger.error(e)
        return False

    return True


def influx_success(self, data: str):
    logger.info(f"Successfully wrote batch: data: {data}")


def influx_error(self, data: str, exception: influx.InfluxDBError):
    logger.error(f"Failed writing batch: config: {self}, data: {data} due: {exception}")


def influx_retry(self, data: str, exception: influx.InfluxDBError):
    logger.debug(
        f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}"
    )


def register_spectra_in_influxdb(data_file_uri: str, spectrometer: str) -> bool:
    """Add a reference to a spectral data file to the InfluxDB.

    Args:
        data_file_uri (str): URI to the data file.
        spectrometer (str): Name of the spectrometer.

    Returns:
        bool: If the reference was successfully added.
    """
    point = (
        influx.Point(INFLUXDB_MEASUREMENT_NAME)
        .tag(INFLUXDB_SPECTROMETER_TAG_NAME, spectrometer)
        .field(INFLUXDB_SPECTROMETER_FILE_PATH_FIELD_NAME, data_file_uri)
    )

    access_token = os.getenv(INFLUXDB_TOKEN_ENV_KEY)
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
        org="",
        token=access_token,
        database=INFLUXDB_DATABASE,
        write_client_options=options,
    ) as client:
        pass

    return True
