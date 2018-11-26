import argparse
import multiprocessing as mp
import os
import pathlib
import warnings
from functools import partial
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import xarray as xr
from filelogging import read_log, write_log


def _combine_forecast(
        files: list,
        output_dir: Union[str, os.PathLike],
        forecast: str,
        filetype: str,
        file_label: str,
        features: list = None,
        log_db: str = None
):
    try:
        # Concatenate along time axis for each forecast and write to netcdf
        # with optional subsetting to features
        forecast_ds = xr.open_mfdataset(
            files,
            concat_dim='time',
            coords='minimal'
        )
        ref_time = pd.to_datetime(forecast_ds['reference_time'].values[0])
        ref_time = ref_time.strftime('%Y-%m-%d_%H-%M-%S')
        filepath = output_dir + '/' + \
                   file_label + \
                   ref_time + '.nc'

        if features is not None:
            forecast_ds = forecast_ds.sel(feature_id=features)

        # Cast time as an integer leadtime to ease concatenation of multiple
        # forecasts
        leadtime = (forecast_ds['time'] - forecast_ds['reference_time'])[:, 0]
        forecast_ds['time'] = leadtime
        forecast_ds.rename({'time': 'leadtime'},inplace=True)

        forecast_ds.to_netcdf(path=filepath, unlimited_dims='reference_time')

        if log_db is not None:
            write_log(sqlite_path=log_db,
                      forecast=forecast,
                      filetype=filetype,
                      files=files,
                      failed=False)
    except Exception as e:
        if log_db is not None:
            write_log(sqlite_path=log_db,
                      forecast=forecast,
                      filetype=filetype,
                      files=files,
                      failed=True)
        warnings.warn('Worker failed with error (' + str(e) + ')')


def _set_times(ds):
    time = ds.attrs['sliceCenterTimeUTC']
    time = time.replace('_', ' ')
    time = pd.to_datetime(time)
    ds['time'] = time
    return ds


def files_to_timeslice(files: list,
                       output_dir: Union[str, os.PathLike],
                       file_label: str = None,
                       log_db: str = None):
    try:
        ds = xr.open_mfdataset(files,
                               preprocess=_set_times)

        # rename_variables
        ds = ds.rename({'discharge': 'streamflow', 'stationId': 'site_no'})

        # Load to dataframe
        variables = ['site_no', 'time', 'streamflow', 'discharge_quality']
        df = ds[variables].load().to_dataframe()

        # site_no
        site_no = df['site_no'].str.decode('utf-8')
        df['site_no'] = site_no

        # Set and sort indexes
        df.set_index(['time', 'site_no'], inplace=True)
        df.sort_index(level=['time', 'site_no'], ascending=[True, True],
                      inplace=True)

        # Screen out fill values
        # Replace -99999 values with nan
        df['streamflow'].replace(-999999, np.nan, inplace=True)

        # Get labe lfor file based on time
        start_time = str(df.index.get_level_values(0).min())
        filepath = output_dir + '/' + file_label + start_time + '.nc'

        df.to_xarray().to_netcdf(filepath)

        if log_db is not None:
            write_log(sqlite_path=log_db,
                      forecast='null',
                      filetype='timeslice',
                      files=files,
                      failed=False)
    except Exception as e:
        if log_db is not None:
            write_log(sqlite_path=log_db,
                      forecast='null',
                      filetype='timeslice',
                      files=files,
                      failed=True)
        warnings.warn('Worker failed with error (' + str(e) + ')')


def files_to_forecast(
        files: list,
        output_dir: Union[str,os.PathLike],
        forecast: str,
        filetype: str,
        file_label: str = None,
        features: list = None,
        ncores: int = None,
        log_db: str = None
):

    # Setup default arguments
    if ncores is None:
        ncores = mp.cpu_count()
    if file_label is None:
        file_label = ''

    # Create dictionary of forecasts, i.e. reference times
    print('Sorting files by forecast init time')
    ds_dict = dict()
    for a_file in files:
        try:
            ds = xr.open_dataset(a_file)

            ref_time = str(ds['reference_time'].values[0])
            if ref_time in ds_dict:
                # append the new number to the existing array at this slot
                ds_dict[ref_time].append(str(a_file))
            else:
                # create a new array in this slot
                ds_dict[ref_time] = [str(a_file)]
        except Exception as e:
            warnings.warn(str(e))

    # Convert dictionary of lists into list of lists for pool mapping
    files_list=[]
    for key, value in ds_dict.items():
        files_list.append(value)

    # Fill in arguments to worker function
    worker_func = partial(_combine_forecast,
                          output_dir=output_dir,
                          forecast=forecast,
                          filetype=filetype,
                          file_label=file_label,
                          features=features,
                          log_db=log_db)

    print('Combing files and writing to new netcdf')
    # multiprocessing
    with mp.Pool(processes=ncores) as pool:
        pool.map(worker_func, files_list)


def main():
    parser = argparse.ArgumentParser(
        description='Combine NWM channel_rt files into 1 file per forecast.'
    )
    parser.add_argument(
        '--input_dir',
        action='store',
        help='Directory containing input files'
    )
    parser.add_argument(
        '--output_dir',
        action='store',
        help='Directory to hold output files'
    )
    parser.add_argument(
        '--filetype',
        action='store',
        help='The filetype, e.g. timeslice, channel_rt'
    )
    parser.add_argument(
        '--forecast',
        action='store',
        help='The forecast range, e.g. analysis_assim, short_range'
    )
    parser.add_argument(
        '--gages_only',
        action='store_true',
        help='Subset channel_rt files to gage points'
    )
    parser.add_argument(
        '--log_files',
        action='store_true',
        help='Log processed and failed files in a sqlite database'
    )
    parser.add_argument(
        '--ncores',
        action='store',
        type=int,
        help='Number of cores for multiprocessing. Defaults to ncores - 1'
    )
    args = parser.parse_args()

    # User inputs
    output_dir = args.output_dir
    filetype = args.filetype
    forecast = args.forecast
    ncores = args.ncores
    input_dir = args.input_dir
    gages_only = args.gages_only
    log_files = args.log_files

    # Get module path
    module_path = str(pathlib.Path(__file__).parent)

    # Get files list and open routelink
    if filetype == 'timeslice':
        pattern = '*00:00*usgsTimeSlice.ncdf'
    else:
        pattern = '*' + forecast + '.' + filetype + '*'

    print(
        'Searching for files in ' +
        input_dir +
        ' matching pattern ' +
        pattern
    )

    files = Path(input_dir).rglob(pattern=pattern)
    files = list(files)
    files = [file for file in files if 'hawaii' not in str(file)]
    print('Found ' + str(len(files)) + ' files')

    # Check files against file log
    if log_files:
        print('Checking files against log...')
        log_db = output_dir + '/filelog.db'
        processed_files = read_log(
            sqlite_path=log_db,
            forecast=forecast,
            filetype=filetype,
            failed=False
        )
        processed_files = set(processed_files)
        files = [pathlib.Path(file) for file in files if
                 str(file) not in processed_files]
        if len(files) == 0:
            print('All files already processed')
            return None
    else:
        log_db = None

    if filetype == 'timeslice':
        print('Combining files')
        files_to_timeslice(
            files=files,
            output_dir=output_dir,
            file_label=filetype + '_',
            log_db=log_db
        )
    else:
        if gages_only:
            print('Combining with subset to gages')
            rl_df = pd.read_csv(module_path + '/data/v2_0_route_link.csv',
                                dtype='str')
            rl_df['feature_id'] = rl_df['feature_id'].astype('int')
            features = list(rl_df['feature_id'])
        else:
            print('Combining without subset')
            features = None

        files_to_forecast(
            files=files,
            output_dir=output_dir,
            forecast=forecast,
            filetype=filetype,
            file_label=forecast + '_' + filetype + '_',
            features=features,
            ncores=ncores,
            log_db=log_db
        )


if __name__ == '__main__':
    main()


