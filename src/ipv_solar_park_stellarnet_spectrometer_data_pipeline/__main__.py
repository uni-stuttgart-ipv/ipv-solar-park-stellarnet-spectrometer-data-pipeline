import os
import logging
import importlib.resources
import time
import datetime as dt
from dataclasses import dataclass
import numpy as np
import pandas as pd
import stellarnet_legacy as sn
from . import spectra, store, stellarnet, LOG_LEVEL_ENV_KEY, LOG_LEVEL_DEFAULT
from .spectra import SpectrometerConfig

LOG_FILE = "ipv_solar_park_stellarnet_spectrometer_data_pipeline.log"
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=LOG_FILE,
    encoding="utf-8",
    level=LOG_LEVEL_DEFAULT,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

STELLARNET_LOG_PATH_PREFIX = "stellarnet"

SPECTRO_VIS_ID = "stellarnet-06041222"
SPECTRO_VIS_CHANNEL = 1

# Calibration coefficients for the uv-vis spectrometer.
# Values found on the bottom label of the spectrometer.
SPECTRO_VIS_C1 = 0.784591
SPECTRO_VIS_C2 = -0.000161
SPECTRO_VIS_C3 = 264.94

SPECTRO_VIS_DARK_COUNTS_PATH = "data/stellarnet-06041222-dark.SSM"
spectro_vis_dark_counts_path = importlib.resources.files(
    "ipv_solar_park_stellarnet_spectrometer_data_pipeline"
).joinpath(SPECTRO_VIS_DARK_COUNTS_PATH)
with importlib.resources.as_file(spectro_vis_dark_counts_path) as path:
    SPECTRO_VIS_DARK_SPECTRUM = stellarnet.load_spectrawiz_spectrum(path)

# Number of channels the spectrometer uses.
SPECTRO_VIS_CHANNELS = 2048
# Desired average dark count threshold.
SPECTRO_VIS_AVG_DARK_COUNT_THRESHOLD = 25
# Minimum total counts to be a valid spectrum.
SPECTRO_VIS_SPECTRA_INTENSITY_THRESHOLD = (
    SPECTRO_VIS_CHANNELS * SPECTRO_VIS_AVG_DARK_COUNT_THRESHOLD
)
# Minimum time between specta.
SPECTRO_VIS_SPECTRA_FREQUENCY_SEC = 10
# Maximum time between spectra.
SPECTRO_VIS_MAX_TIME_BETWEEN_SPECTRA_SEC = 30 * 60
# Minimum difference to trigger a dynamic spectrum.
SPECTRO_VIS_SPRECTRA_DIFF_THRESHOLD = 0.05


@dataclass
class MeasurementConfig:
    # Minimum total counts to be a valid spectra.
    intensity_threshold: int
    # Minimum time between specta.
    spectra_freq_sec: int
    # Maximum time between specta.
    max_period_between_spectra_sec: int
    # Minimum difference to trigger a dynamic spectra.
    diff_threshold: float


class Spectrometer:
    def __init__(
        self,
        id: str,
        device_config: SpectrometerConfig,
        measurement_config: MeasurementConfig,
        dark_spectra: np.ndarray,
    ):
        self.active = True
        self.device_config = device_config
        self.measurement_config = measurement_config
        self._id = id
        self._dark_spectra = dark_spectra
        self._last_spectra_time: None | dt.datetime = None
        self._last_spectra: None | np.ndarray = None

    @property
    def id(self) -> str:
        return self._id

    def min_time_between_spectra_has_elapsed(self, time: dt.datetime) -> bool:
        if self._last_spectra_time is None:
            return True

        elapsed = time - self._last_spectra_time
        return elapsed.total_seconds() >= self.measurement_config.spectra_freq_sec

    def max_time_between_spectra_has_elapsed(self, time: dt.datetime) -> bool:
        if self._last_spectra_time is None:
            return True

        elapsed = time - self._last_spectra_time
        return (
            elapsed.total_seconds()
            >= self.measurement_config.max_period_between_spectra_sec
        )

    def spectrum_surpasses_intensity_threshold(self, spectrum: np.ndarray) -> bool:
        dark_interp = np.interp(
            spectrum[0], self._dark_spectra[0], self._dark_spectra[1]
        )
        counts_corr = spectrum[1] - dark_interp
        return counts_corr.sum() >= self.measurement_config.intensity_threshold

    def spectrum_surpasses_difference_threshold(self, spectrum: np.ndarray) -> bool:
        if self._last_spectra is None:
            return True
        counts_last = self._last_spectra[1]

        counts = spectrum[1]
        rel_diff = (counts - counts_last) / counts_last.sum()
        return rel_diff.sum() >= self.measurement_config.diff_threshold

    def spectrum_should_be_stored(
        self, time: dt.datetime, spectrum: np.ndarray
    ) -> bool:
        if not self.spectrum_surpasses_intensity_threshold(spectrum):
            logger.debug(
                f"[spectrometer {self.id}] spectrum does not surpass intensity threshold"
            )
            return False
        if self.max_time_between_spectra_has_elapsed(time):
            return True

        return self.spectrum_surpasses_difference_threshold(spectrum)

    def set_last_spectrum(self, time: dt.datetime, spectrum: np.ndarray):
        self._last_spectra_time = time
        self._last_spectra = spectrum


SPECTROMETERS = [
    Spectrometer(
        SPECTRO_VIS_ID,
        SpectrometerConfig(
            SPECTRO_VIS_CHANNEL, SPECTRO_VIS_C1, SPECTRO_VIS_C2, SPECTRO_VIS_C3
        ),
        MeasurementConfig(
            SPECTRO_VIS_SPECTRA_INTENSITY_THRESHOLD,
            SPECTRO_VIS_SPECTRA_FREQUENCY_SEC,
            SPECTRO_VIS_MAX_TIME_BETWEEN_SPECTRA_SEC,
            SPECTRO_VIS_SPRECTRA_DIFF_THRESHOLD,
        ),
        SPECTRO_VIS_DARK_SPECTRUM,
    )
]


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


def main():
    """Acquire and store spectra."""
    sn.init()
    sn.enable_logging(stellarnet_log_path(STELLARNET_LOG_PATH_PREFIX))
    sn.open()

    device_count_expected = len(list(filter(lambda s: s.active, SPECTROMETERS)))
    device_count = sn.device_count()
    if device_count != device_count_expected:
        raise RuntimeError(
            f"expected {device_count_expected} spectrometers, but found {device_count}"
        )

    last_log_check = dt.datetime.now()
    log_timestamp = dt.datetime.now()
    while True:
        set_log_level()
        time.sleep(5)
        now = dt.datetime.now()
        if now - last_log_check > dt.timedelta(hours=1):
            if now.month != log_timestamp.month:
                sn.enable_logging(stellarnet_log_path(STELLARNET_LOG_PATH_PREFIX))

        for s in SPECTROMETERS:
            if not s.min_time_between_spectra_has_elapsed(dt.datetime.now()):
                continue

            spectrum = spectra.acquire_spectra(s.device_config)
            timestamp = dt.datetime.now()
            if isinstance(spectrum, sn.ScanStatus):
                logger.error(
                    f"scan for spectrometer {s.id} could not be acquired: status {spectrum}"
                )
                continue

            if s.spectrum_should_be_stored(timestamp, spectrum):
                spectrum_df = pd.Series(spectrum[1], index=spectrum[0])
                s3_key = store.store_spectra_in_s3(spectrum_df, timestamp, s.id)
                if s3_key is None:
                    logger.error("could not store spectrum in S3")
                    continue

                store.register_spectra_in_influxdb(timestamp, s3_key, s.id)
                s.set_last_spectrum(timestamp, spectrum)
                logger.info(f"spectrum stored for {s.id}")


if __name__ == "__main__":
    logger.info("running")
    main()
