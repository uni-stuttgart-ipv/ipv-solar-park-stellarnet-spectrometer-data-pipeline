import numpy as np
import stellarnet_legacy as sn


class SpectrometerConfg:
    def __init__(self, channel: int, c1: float, c2: float, c3: float):
        self._channel = 1
        self._calibration_coeffs = sn.CalibrationCoefficients(c1, c2, c3)

        self.active = True

    @property
    def channel(self) -> int:
        return self._channel

    def wavelengths(self) -> np.ndarray:
        return self._calibration_coeffs.wavelengths()


# def initialize_spectrometer(spectrometer: SpectrometerConfg):
#     sn.initialize_spectrometer(
#         100, 5, sn.SmoothingWindow.NONE, sn.XTiming.MEDIUM, sn.TemperatureCompensation.OFF
#     )


def acquire_spectra(spectrometer: SpectrometerConfg) -> sn.ScanStatus | np.ndarray:
    data = sn.read_spectrometer_c(spectrometer.channel)
    if isinstance(data, sn.ScanStatus):
        return data
    else:
        return np.stack((spectrometer.wavelengths(), data))
