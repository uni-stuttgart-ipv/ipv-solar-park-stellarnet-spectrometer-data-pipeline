from __future__ import annotations

import datetime as dt
import json
import logging
import os
from dataclasses import dataclass

import numpy as np

from . import stellarnet
from .spectra import SpectrometerConfig


@dataclass
class MeasurementConfig:
    # Minimum total counts to be a valid spectra.
    intensity_threshold: int
    # Minimum time between specta.
    spectra_freq_sec: int
    # Maximum time between specta.
    max_period_between_spectra_sec: int
    # Minimum difference to trigger a dynamic spectra.
    spectral_diff_threshold: float


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
        self._dark_spectrum = dark_spectra
        self._last_spectrum_time: None | dt.datetime = None
        self._last_spectrum: None | np.ndarray = None

    @property
    def id(self) -> str:
        return self._id

    def min_time_between_spectra_has_elapsed(self, time: dt.datetime) -> bool:
        if self._last_spectrum_time is None:
            return True

        elapsed = time - self._last_spectrum_time
        return elapsed.total_seconds() >= self.measurement_config.spectra_freq_sec

    def max_time_between_spectra_has_elapsed(self, time: dt.datetime) -> bool:
        if self._last_spectrum_time is None:
            return True

        elapsed = time - self._last_spectrum_time
        return (
            elapsed.total_seconds()
            >= self.measurement_config.max_period_between_spectra_sec
        )

    def spectrum_surpasses_intensity_threshold(self, spectrum: np.ndarray) -> bool:
        dark_interp = np.interp(
            spectrum[0], self._dark_spectrum[0], self._dark_spectrum[1]
        )
        peak = (spectrum[1] - dark_interp).max()
        return peak >= self.measurement_config.intensity_threshold

    def spectrum_surpasses_difference_threshold(self, spectrum: np.ndarray) -> bool:
        if self._last_spectrum is None:
            return True

        counts_last = self._last_spectrum[1]
        counts = spectrum[1]
        rel_diff = np.sqrt(np.square(counts - counts_last).sum())
        rel_diff = rel_diff / counts_last.sum()
        return rel_diff >= self.measurement_config.spectral_diff_threshold

    def spectrum_should_be_stored(
        self,
        time: dt.datetime,
        spectrum: np.ndarray,
        logger: logging.Logger | None = None,
    ) -> bool:
        intensity_threshold_exceeded = self.spectrum_surpasses_intensity_threshold(
            spectrum
        )
        if logger is not None:
            log_data(
                logger,
                logging.DEBUG,
                self,
                {
                    "event": "spectrum_validation",
                    "property": "intensity_threshold",
                    "value": intensity_threshold_exceeded,
                },
            )
        if not intensity_threshold_exceeded:
            return False

        max_time_exceeded = self.max_time_between_spectra_has_elapsed(time)
        if logger is not None:
            log_data(
                logger,
                logging.DEBUG,
                self,
                {
                    "event": "spectrum_validation",
                    "property": "spectral_diff_threshold",
                    "value": max_time_exceeded,
                },
            )
        if max_time_exceeded:
            return True

        diifference_threshold_exceeded = self.spectrum_surpasses_difference_threshold(
            spectrum
        )
        if logger is not None:
            log_data(
                logger,
                logging.DEBUG,
                self,
                {
                    "event": "spectrum_validation",
                    "property": "spectral_diff_threshold",
                    "value": diifference_threshold_exceeded,
                },
            )
        return diifference_threshold_exceeded

    def set_last_spectrum(self, time: dt.datetime, spectrum: np.ndarray):
        self._last_spectrum_time = time
        self._last_spectrum = spectrum


def load_spectrometer_config(path: os.PathLike) -> list[Spectrometer]:
    with open(path) as f:
        config = json.load(f)

    if not isinstance(config, list):
        raise TypeError("invalid spectrometer configuration")

    spectrometers = []
    for idx, conf in enumerate(config):
        try:
            cal_coeffs = conf["calibration_coefficients"]
            device_config = SpectrometerConfig(
                int(conf["channel"]),
                int(conf["number_of_pixels"]),
                float(cal_coeffs["c1"]),
                float(cal_coeffs["c2"]),
                float(cal_coeffs["c3"]),
                float(cal_coeffs.get("c4", 0)),
            )
            meas_config = MeasurementConfig(
                int(conf["intensity_counts_threshold"]),
                int(conf["spectra_frequency_sec"]),
                int(conf["max_time_between_spectra_sec"]),
                float(conf["spectra_difference_threshold"]),
            )
            dark_spectra = stellarnet.load_spectrawiz_spectrum(
                conf["dark_spectra_path"]
            )

            spectrometer = Spectrometer(
                str(conf["id"]), device_config, meas_config, dark_spectra
            )
        except KeyError as err:
            err.add_note(f"invalid spectrometer configuration for item {idx + 1}")
            raise

        spectrometers.append(spectrometer)

    return spectrometers


def log_data(
    logger: logging.Logger, level: int, spectrometer: Spectrometer, data: dict
):
    msg = f'"spectrometer": "{spectrometer.id}"'
    for key, value in data.items():
        msg += f', "{key}": "{value}"'
    logger.log(level, msg)
