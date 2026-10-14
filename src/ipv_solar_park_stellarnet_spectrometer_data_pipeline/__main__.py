from __future__ import annotations

import datetime as dt
import logging
import os
import time
from argparse import ArgumentParser
from enum import Enum

import pandas as pd
import stellarnet_legacy as sn

from . import LOG_LEVEL_DEFAULT, LOG_LEVEL_ENV_KEY, lib, notify, spectra, store


class StoreAction(Enum):
    REMOTE = 0
    REMOTE_NO_INFLUX = 1  # do not register spectra in influx
    REMOTE_NO_STORE = 2  # do not store spectra in s3 or register them in influx
    LOCAL = 3  # store data locally


LOG_FILE = "ipv_solar_park_stellarnet_spectrometer_data_pipeline.log"
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=LOG_FILE,
    encoding="utf-8",
    level=LOG_LEVEL_DEFAULT,
    format='{"time"="%(asctime)s", %(message)s}',
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOOP_DELAY = 5
STELLARNET_LOG_PATH_PREFIX = "stellarnet"


def stellarnet_log_path(prefix: str) -> str:
    now = dt.datetime.now()
    timestamp = now.strftime("%Y-%m")
    return f"{prefix}-{timestamp}.log"


def set_log_level():
    """Set the log level from the environment variable."""
    log_level = os.getenv(LOG_LEVEL_ENV_KEY)
    if log_level is None:
        return

    level: int | None = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        logger.warning(f"invalid log level: {log_level}")
        return

    logger.setLevel(level)


def run(
    spectrometers: list[lib.Spectrometer],
    store_action: StoreAction = StoreAction.REMOTE,
    local_path: None | os.PathLike = None,
):
    """Acquire and store spectra

    Args:
        spectrometers (list[Spectrometer]): Spectrometers.
        store_action (StoreAction): Action to take when storing spectra. Defaults to StoreAction.Store.
        local_path (None | os.PathLike): Path to locally to store spectra. Only valid if `store_action` is `StoreAction.Local`. Defaults to None.

    Raises:
        RuntimeError: Unexpected number of devices found.
    """
    if store_action is StoreAction.LOCAL and local_path is None:
        raise RuntimeError("local path must be provided for local storage")

    sn.init()
    sn.enable_logging(stellarnet_log_path(STELLARNET_LOG_PATH_PREFIX))
    sn.open()

    device_count_expected = len(list(filter(lambda s: s.active, spectrometers)))
    device_count = sn.device_count()
    if device_count != device_count_expected:
        raise RuntimeError(
            f"expected {device_count_expected} spectrometers, but found {device_count}"
        )

    last_log_check = dt.datetime.now()
    log_timestamp = dt.datetime.now()
    while True:
        set_log_level()
        time.sleep(LOOP_DELAY)
        now = dt.datetime.now()
        if (
            now - last_log_check > dt.timedelta(hours=1)
            and now.month != log_timestamp.month
        ):
            sn.enable_logging(stellarnet_log_path(STELLARNET_LOG_PATH_PREFIX))

        for spectrometer in spectrometers:
            if not spectrometer.min_time_between_spectra_has_elapsed(now):
                continue

            spectrum = spectra.acquire_spectra(spectrometer.device_config)
            timestamp = dt.datetime.now()
            if isinstance(spectrum, sn.ScanStatus):
                lib.log_data(
                    logger,
                    logging.ERROR,
                    spectrometer,
                    {"event": "acquisition_failure"},
                )
                continue

            if spectrometer.spectrum_should_be_stored(
                timestamp, spectrum, logger=logger
            ):
                spectrum_df = pd.Series(spectrum[1], index=spectrum[0])
                if (
                    store_action is StoreAction.REMOTE
                    or store_action is StoreAction.REMOTE_NO_INFLUX
                ):
                    s3_key = store.store_spectra_in_s3(
                        spectrum_df, timestamp, spectrometer.id
                    )
                    if s3_key is None:
                        lib.log_data(
                            logger,
                            logging.ERROR,
                            spectrometer,
                            {"event": "storage_failure"},
                        )
                        continue

                    if store_action is not StoreAction.REMOTE_NO_INFLUX:
                        store.register_spectra_in_influxdb(
                            timestamp, s3_key, spectrometer.id
                        )
                if store_action is StoreAction.LOCAL:
                    assert local_path is not None
                    store.store_spectra_local(
                        spectrum_df, timestamp, spectrometer.id, local_path
                    )

                spectrometer.set_last_spectrum(timestamp, spectrum)
                lib.log_data(
                    logger, logging.INFO, spectrometer, {"event": "spectrum_stored"}
                )


def test_notify():
    notify_credentials = notify.get_credentials()
    if notify_credentials is None:
        missing = [
            cred
            for cred in (
                notify.ENV_KEY_EMAIL,
                notify.ENV_KEY_USERNAME,
                notify.ENV_KEY_PASSWORD,
            )
            if os.getenv(cred) is None
        ]
        raise RuntimeError(f"notification credential(s) {', '.join(missing)} missing")

    msg = "test notification"
    notify.send_error_email(notify_credentials, msg)
    print("test notification sent")


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "spectrometers",
        type=str,
        nargs="?",
        help="path to spectrometers configuration file",
    )

    args_exc = parser.add_mutually_exclusive_group()
    args_exc.add_argument(
        "--no-store",
        action="store_true",
        help="do not store spectra",
    )
    args_exc.add_argument(
        "--no-influx",
        action="store_true",
        help="store spectra in S3, but do not register them in InfluxDB",
    )
    args_exc.add_argument(
        "--store-local", type=str, nargs="?", const=".", help="store spectra locally"
    )
    args_exc.add_argument(
        "--test-notify", action="store_true", help="send a test notification email"
    )

    args = parser.parse_args()

    if args.test_notify:
        if args.spectrometers:
            parser.error(
                "--test-notify can not be provided with a spectrometer configuration file"
            )

        test_notify()
        return

    if args.spectrometers is None:
        parser.error("missing spectrometer configuration file path")

    store_action = StoreAction.REMOTE
    local_path = None
    if args.no_store:
        store_action = StoreAction.REMOTE_NO_STORE
    elif args.no_influx:
        store_action = StoreAction.REMOTE_NO_INFLUX
    elif args.store_local is not None:
        store_action = StoreAction.LOCAL
        local_path = args.store_local

    spectrometers = lib.load_spectrometer_config(args.spectrometers)
    run(spectrometers, store_action, local_path)


if __name__ == "__main__":
    logger.info('"event": "script_start"')
    main()
