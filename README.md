# ipv-solar_park-stellarnet_spectrometer-data_pipeline

Data pipeline for the Stellarnet spectrometers in the IPV Solar Park (rooftop).

Monitors solar spectrum and saves on either
+ Every 10 minutes
+ If there is a significant change in the spectrum.

Data is saved to the S3 bucket XXX and registered in InfluxDB.

## License

`ipv-solar-park-stellarnet-spectrometer-data-pipeline` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
