import argparse
import pathlib
import subprocess

from filelogging import read_log


def main():
    parser = argparse.ArgumentParser(
        description='Combine NWM channel_rt files into 1 file per forecast.'
    )
    parser.add_argument(
        '--input_dir',
        default='/d9/jmills/ingest/operationalIngests/v2.0/',
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
    args = parser.parse_args()

    # User inputs
    input_dir = args.input_dir
    output_dir = args.output_dir
    filetype = args.filetype
    forecast = args.forecast
    gages_only = args.gages_only
    log_files = False

    # Get module directory
    module_path = str(pathlib.Path(__file__).absolute().parent) + \
                  '/processing.py'

    daily_dirs = list(pathlib.Path(input_dir).glob('nwm.20*'))
    print('Found {0} daily directories, checking against log '
          'and submitting to pbs'.
          format(str(len(daily_dirs))))
    sqlite_path = output_dir + '/filelog.db'

    for a_dir in daily_dirs:
        # Check files against file log
        if filetype == 'timeslice':
            pattern = '*00:00*usgsTimeSlice.ncdf'
        else:
            pattern = '*' + forecast + '.' + filetype + '*'

        files = pathlib.Path(a_dir).rglob(pattern)
        files = [str(file) for file in files]

        if log_files:
            processed_files = read_log(
                sqlite_path=sqlite_path,
                forecast=forecast,
                filetype=filetype,
                failed=False
            )
            processed_files = set(processed_files)
            files = [pathlib.Path(file) for file in files if
                     str(file) not in processed_files]
        if len(files) > 0:
            print('Found {0} files not processed in daily directory {1}'.
                  format(str(len(files)), str(a_dir)))
            python_cmd = 'export PATH=' \
                         '/d9/jmills/miniconda3/envs/nwm_analysis/bin:$PATH;'
            python_cmd += 'export PYTHONPATH=' \
                          '/d9/jmills/miniconda3/envs/nwm_analysis/lib/python3.6/' \
                          'site-packages/;'
            python_cmd += 'export HDF5_USE_FILE_LOCKING=FALSE;'
            python_cmd += 'python {0} ' \
                          '--input_dir {1} --output_dir {2} --filetype {3} ' \
                          '--forecast {4} '
            if gages_only:
                python_cmd += '--gages_only '
            if log_files:
                python_cmd += '--log_files '
            python_cmd += '--ncores 4'
            python_cmd = python_cmd.format(module_path, a_dir, output_dir, filetype, forecast)

            qsub_cmd = 'echo "{0}" | ' \
                       'qsub -l nodes=hydro-c1-node5:ppn=4,walltime=00:30:00 -N {1}'
            qsub_cmd = qsub_cmd.format(python_cmd, 'nwm_processing')

            subprocess.run(qsub_cmd, shell=True)
        else:
            print('All files already processed in daily directory {0}'.
                  format(str(a_dir)))
            pass


if __name__ == '__main__':
    main()
