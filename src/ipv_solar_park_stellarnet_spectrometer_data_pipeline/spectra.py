from __future__ import annotations

import numpy as np
import stellarnet_legacy as sn


class SpectrometerConfig:
    def __init__(
        self,
        channel: int,
        pixels: int,
        c1: float,
        c2: float,
        c3: float,
        c4: float = 0.0,
    ):
        """Configuration parameters of a spectrometer.

        Args:
            channel (int): Communication channel.
            pixels (int): Number of pixels.
            c1 (float): c1 calibration coefficient.
            c2 (float): c2 calibration coefficient.
            c3 (float): c3 calibration coefficient.
            c4 (float, optional): c4 calibration coefficient. Defaults to 0.
        """
        self._channel = channel
        self._pixels = pixels
        self._calibration_coeffs = sn.CalibrationCoefficients(c1, c2, c3, c4)

    @property
    def channel(self) -> int:
        return self._channel

    @property
    def pixels(self) -> int:
        return self._pixels

    def wavelengths(self) -> np.ndarray:
        return self._calibration_coeffs.wavelengths(self._pixels)


def acquire_spectra(spectrometer: SpectrometerConfig) -> sn.ScanStatus | np.ndarray:
    """Acquire a spectra.

    Returns:
        sn.ScanStatus | np.ndarray: If a spectra is acquired successfully, returns the xy data of wavelength and counts.
        If an error occurs, returns the scan status.
    """
    data = sn.read_spectrometer_c(spectrometer.channel)
    if isinstance(data, sn.ScanStatus):
        return data
    else:
        wavelengths = spectrometer.wavelengths()
        counts = data[: spectrometer.pixels]
        return np.stack((wavelengths, counts))


# NB: included in demo programs from StellarNet, but doesn't seemto be needed.
# def initialize_spectrometer(spectrometer: SpectrometerConfg):
#     sn.initialize_spectrometer(
#         100, 5, sn.SmoothingWindow.NONE, sn.XTiming.MEDIUM, sn.TemperatureCompensation.OFF
#     )
