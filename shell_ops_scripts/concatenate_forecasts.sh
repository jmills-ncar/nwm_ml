#!/usr/bin/env bash

forecast=$1

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/reforecast_v2.0/$forecast/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/reforecast_v2.0/ \
--filetype channel_rt \
--forecast $forecast

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/reforecast_v1.2.1/$forecast/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/reforecast_v1.2.1/ \
--filetype channel_rt \
--forecast $forecast

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1/$forecast/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1/ \
--filetype channel_rt \
--forecast $forecast

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v2.0/$forecast/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v2.0/ \
--filetype channel_rt \
--forecast $forecast

python concatenate.py \
--input_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1_historical/$forecast/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1_historical/ \
--filetype channel_rt \
--forecast $forecast