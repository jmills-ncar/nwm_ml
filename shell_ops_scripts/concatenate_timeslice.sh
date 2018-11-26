#!/usr/bin/env bash

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1/timeslice/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1/ \
--filetype timeslice \
--forecast null

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v2.0/timeslice/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v2.0/ \
--filetype timeslice \
--forecast null

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1_historical/timeslice/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1_historical/ \
--filetype timeslice \
--forecast null