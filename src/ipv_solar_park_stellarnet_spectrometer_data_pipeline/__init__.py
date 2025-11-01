# SPDX-FileCopyrightText: 2025-present Brian Carlsen <carlsen.bri@gmail.com>
#
# SPDX-License-Identifier: MIT
import datetime as dt
import stellarnet_legacy as sn
from . import spectra
from .spectra import SpectrometerConfg

STELLARNET_LOG_PATH_PREFIX = "stellarnet"

# Calibration coefficients for the uv-vis spectrometer.
# Values found on the bottom label of the spectrometer.
SPECTRO_VIS_CHANNEL = 1
SPECTRO_VIS_C1 = 0.784591
SPECTRO_VIS_C2 = -0.000161
SPECTRO_VIS_C3 = 264.94
SPECTROMETERS = [
    SpectrometerConfg(
        SPECTRO_VIS_CHANNEL, SPECTRO_VIS_C1, SPECTRO_VIS_C2, SPECTRO_VIS_C3
    )
]


def stellarnet_log_path(prefix: str) -> str:
    now = dt.datetime.now()
    timestamp = now.strftime("%Y-%m")
    return f"{prefix}-{timestamp}.log"


def main():
    """Acquires and stores spectra."""
    sn.init()
    sn.enable_logging(STELLARNET_LOG_PATH_PREFIX)
    sn.open()

    device_count_expected = len(list(filter(lambda s: s.active, SPECTROMETERS)))
    device_count = sn.device_count()
    if device_count != device_count_expected:
        raise RuntimeError(
            f"expected {device_count_expected} spectrometers, but found {device_count}"
        )

    for s in SPECTROMETERS:
        spectra.acquire_spectra(s)
