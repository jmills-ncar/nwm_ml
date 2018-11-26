#!/usr/bin/env bash
forecast=$1

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/reforecast_v2.0/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/reforecast_v2.0/$forecast/ \
--filetype channel_rt \
--forecast $forecast \
--gages_only

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/reforecast_v1.2.1/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/reforecast_v1.2.1/$forecast/ \
--filetype channel_rt \
--forecast $forecast \
--gages_only

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/v1.2.1/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1/$forecast/ \
--filetype channel_rt \
--forecast $forecast \
--gages_only

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/v2.0/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v2.0/$forecast/ \
--filetype channel_rt \
--forecast $forecast \
--gages_only

python bulk_forecast_processing.py \
--input_dir /d9/jmills/ingest/operationalIngests/v1.2.1_historical/ \
--output_dir /d9/jmills/ingest/operationalIngests/combined_forecasts/v1.2.1_historical/$forecast/ \
--filetype channel_rt \
--forecast $forecast \
--gages_only