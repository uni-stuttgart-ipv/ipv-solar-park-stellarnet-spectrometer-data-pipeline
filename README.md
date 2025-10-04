# ipv-solar_park-stellarnet_spectrometer-data_pipeline

Data pipeline for the Stellarnet spectrometers in the IPV Solar Park (rooftop).

Monitors solar spectrum and saves on either
+ Every 10 minutes
+ If there is a significant change in the spectrum.

Data is saved to the S3 bucket `solar-park-spectra` and registered in InfluxDB.

## Setup

### AWS access key
Data is stored in the `solar-park-spectra` S3 bucket. To configure access to this set the environment variables
+ `SOLAR_PARK_SPECTRA_AWS_ACCESS_KEY_ID` to the access key id
+ `SOLAR_PARK_SPECTRA_AWS_SECRET_ACCESS_KEY` to the secret access key

### InfluxDB
Set the environment variables
+ `SOLAR_PARK_SPECTRA_INFLUXDB_TOKEN` to the influx db access token

## License

`ipv-solar-park-stellarnet-spectrometer-data-pipeline` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
