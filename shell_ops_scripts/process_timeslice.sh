#!/usr/bin/env bash

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/v1.2.1/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1/timeslice/ \
--filetype timeslice \
--forecast null \
--gages_only

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/v2.0/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v2.0/timeslice/ \
--filetype timeslice \
--forecast null \
--gages_only

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/v1.2.1_historical/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1_historical/timeslice/ \
--filetype timeslice \
--forecast null \
--gages_only