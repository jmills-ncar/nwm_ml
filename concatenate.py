import os
from argparse import ArgumentParser
from pathlib import Path
from typing import Union

import xarray as xr


def concatenate_forecasts(files: list,
                          output_path: Union[str, os.PathLike]):
    ds = xr.open_mfdataset(
        files,
        concat_dim='reference_time',
        chunks=None,
        decode_cf=False,
        parallel=True,
        autoclose=False
    )
    ds.load().to_netcdf(output_path)


def concatenate_timeslices(files: list,
                          output_path: Union[str, os.PathLike]):
    ds = xr.open_mfdataset(
        files,
        concat_dim='time',
        decode_cf=False,
        parallel=True,
        autoclose=True
    )
    ds.to_netcdf(output_path)


def main():
    parser = ArgumentParser(
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
        help='Filepath for new netcdf file containing concatenated forecasts'
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
    args = parser.parse_args()

    # User inputs
    input_dir = args.input_dir
    output_dir = args.output_dir
    filetype = args.filetype
    forecast = args.forecast

    # Get files list
    if filetype == 'timeslice':
        pattern = '*timeslice*.nc'
        output_path = str(output_dir) + '/' + filetype + '_concat.nc'
    else:
        pattern = '*' + forecast + '_' + filetype + '*.nc'
        output_path = str(output_dir) + '/' + forecast + \
            '_' + filetype + '_concat.nc'

    print(
        'Searching for files in ' +
        input_dir +
        ' matching pattern ' +
        pattern
    )

    files = Path(input_dir).rglob(pattern=pattern)
    files = list(files)
    files = [file for file in files if 'hawaii' not in str(file)]

    print('Concatenating files and outputting to ' + str(output_path))
    if filetype == 'timeslice':
        concatenate_timeslices(files=files, output_path=output_path)
    else:
        concatenate_forecasts(files=files, output_path=output_path)


if __name__ == '__main__':
    main()
