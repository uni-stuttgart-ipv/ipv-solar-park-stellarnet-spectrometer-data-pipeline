# ipv-solar_park-stellarnet_spectrometer-data_pipeline

Data pipeline for the Stellarnet spectrometers in the IPV Solar Park (rooftop).

> For access to our solar spectrum data send an email to sekretariat@ipv.uni-stuttgart.de.

Monitors solar spectrum and saves if
+ A minimum intensity threshold is surpassed (to avoid taking spetra at night)
+ A maximum time threshold has been surpassed
+ A significant change in the spectrum occurs

Data is saved to the S3 bucket `solar-park-spectra` and registered in InfluxDB.

## Setup

### AWS access key (required)
Data is stored in the `solar-park-spectra` S3 bucket. To configure access to this set the environment variables
+ `SOLAR_PARK_SPECTRA_AWS_ACCESS_KEY_ID` to the access key id
+ `SOLAR_PARK_SPECTRA_AWS_SECRET_ACCESS_KEY` to the secret access key

### InfluxDB (required)
Set the environment variables
+ `SOLAR_PARK_SPECTRA_INFLUXDB_TOKEN` to the influx db access token

### Notifications (optional)
Emails can be sent if an error occurs. To configure this set the environment variables
+ `SOLAR_PARK_SPECTRA_NOTIFY_EMAIL` to the email that should receive the notification (e.g. `first.last@ipv.uni-stuttgart.de`)
+ `SOLAR_PARK_SPECTRA_NOTIFY_USERNAME` to the username of the email account to send from (`ac` account)
+ `SOLAR_PARK_SPECTRA_NOTIFY_PASSWORD` to the password of the email account (password you use with your `ac` account)

### Logging
You can change the log level by setting the environment variable `IPV_SOLAR_PARK_STELLARNET_SPECTROMETER_DATA_PIPELINE_LOG_LEVEL` to a valid Python `logging` log level. i.e. `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` (case insensitive). 

### Running the program
If this package is installed as a Python library (e.g. via `pip install`) it can be run as a script using
```sh
python -m ipv_solar_park_stellarnet_spectrometer_data_pipeline
```

## License

`ipv-solar-park-stellarnet-spectrometer-data-pipeline` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
