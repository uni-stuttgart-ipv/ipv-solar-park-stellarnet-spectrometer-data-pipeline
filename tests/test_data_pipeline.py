import pathlib

import numpy as np

import ipv_solar_park_stellarnet_spectrometer_data_pipeline as pipeline

SPECTROMETER_CONFIG_PATH = (
    pathlib.Path(__file__).parent / "data/spectrometers.config.json"
)


def test_load_spectrometer_configuration():
    config = pipeline.lib.load_spectrometer_config(SPECTROMETER_CONFIG_PATH)
    assert len(config) == 2

    s1_dark = [
        [300.00, 301.00, 302.00, 303.00, 304.00],
        [1.00, 2.00, 3.00, 4.00, 5.00],
    ]
    assert config[0]._id == "s1"
    assert np.all(config[0]._dark_spectrum == s1_dark)
    assert config[0].device_config.channel == 1
    assert config[0].device_config._pixels == 2048
    assert config[0].device_config._calibration_coeffs.c1 == 1
    assert config[0].device_config._calibration_coeffs.c2 == 2
    assert config[0].device_config._calibration_coeffs.c3 == 3
    assert config[0].device_config._calibration_coeffs.c4 == 0
    assert config[0].measurement_config.intensity_threshold == 100
    assert config[0].measurement_config.spectra_freq_sec == 10
    assert config[0].measurement_config.max_period_between_spectra_sec == 60
    assert config[0].measurement_config.spectral_diff_threshold == 0.10

    s2_dark = [
        [900.00, 901.00, 902.00, 903.00, 904.00, 905.00],
        [0.00, 1.00, 2.00, 3.00, 4.00, 5.00],
    ]
    assert config[1]._id == "s2"
    assert np.all(config[1]._dark_spectrum == s2_dark)
    assert config[1].device_config.channel == 2
    assert config[1].device_config._pixels == 512
    assert config[1].device_config._calibration_coeffs.c1 == 2
    assert config[1].device_config._calibration_coeffs.c2 == 4
    assert config[1].device_config._calibration_coeffs.c3 == 8
    assert config[1].device_config._calibration_coeffs.c4 == 0
    assert config[1].measurement_config.intensity_threshold == 200
    assert config[1].measurement_config.spectra_freq_sec == 20
    assert config[1].measurement_config.max_period_between_spectra_sec == 200
    assert config[1].measurement_config.spectral_diff_threshold == 0.2
