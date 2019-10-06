import click
import os
import pickle

import gtfs_tripify as gt


@click.group()
def cli():
    pass


@click.command()
@click.argument('inpath')
@click.argument('outpath')
@click.option('--no-clean', is_flag=True,
              help='Disable cleaning up the logbook before writing it to disk.')
@click.option('--include-timestamp-log', is_flag=True,
              help='Write the timestamp log to disk. Timestamps are used for merging logbooks.')
@click.option('--include-error-log', is_flag=True, help='Write the error log to disk.')
@click.option(
    '--to', default='gtfs', show_default=True, help='The output format. Must be "csv" or "gtfs".'
)
def logify(inpath, outpath, no_clean, include_timestamp_log, include_error_log, to):
    inpath = os.path.expanduser(os.path.abspath(inpath))
    outpath = os.path.expanduser(os.path.abspath(outpath))

    if not os.path.exists(inpath):
        raise ValueError(f'Input directory {inpath!r} does not exist.')
    elif not os.path.isdir(inpath):
        raise ValueError(f'Input directory {inpath!r} is not a directory.')
    elif not os.path.exists(os.path.dirname(outpath)):
        o = os.path.dirname(outpath)
        raise ValueError(f'Output directory {o!r} does not exist.')

    to = to.lower()
    if to != 'csv' and to != 'gtfs':
        raise ValueError(
            f'"to" must be one of "csv" or "gtfs", but the value {to!r} was provided instead.'
        )

    messages = []
    for filename in sorted(os.listdir(inpath)):
        with open(inpath.rstrip('/') + '/' + filename, 'rb') as f:
            messages.append(f.read())

    logbook, timestamp_log, error_log = gt.logify(messages)

    if not no_clean:
        logbook = gt.ops.discard_partial_logs(logbook)
        logbook = gt.ops.cut_cancellations(logbook)

    if to == 'csv':
        gt.ops.to_csv(logbook, outpath)
    else:  # to == 'gtfs'
        gt.ops.to_gtfs(logbook, outpath)

    if include_timestamp_log:
        with open(os.path.dirname(outpath).rstrip('/') + '/timestamps.pkl', 'wb') as fp:
            pickle.dump(timestamp_log, fp)
    if include_error_log:
        with open(os.path.dirname(outpath).rstrip('/') + '/errors.pkl', 'wb') as fp:
            pickle.dump(error_log, fp)


@click.command()
@click.argument('l_fp_1')
@click.argument('l_fp_2')
@click.argument('outpath')
@click.option('--no-clean', help='Disable cleaning up the logbook before writing it to disk.')
@click.option('--include-timestamp-log', is_flag=True,
              help='Write the timestamp log to disk. Timestamps are used for merging logbooks.')
@click.option('--include-error-log', is_flag=True,
              help='Write the timestamp log to disk. Timestamps are used for merging logbooks.')
@click.option(
    '--to', default='gtfs', show_default=True, help='The output format. Must be "csv" or "gtfs".'
)
def merge(l_fp_1, l_fp_2, outpath, no_clean, include_timestamp_log, include_error_log, to):
    l1 = os.path.expanduser(os.path.abspath(l_fp_1))
    l2 = os.path.expanduser(os.path.abspath(l_fp_2))

    if not os.path.exists(l1):
        raise ValueError(f'Input logbook {l1!r} does not exist.')
    elif not os.path.exists(l2):
        raise ValueError(f'Input logbook {l2!r} does not exist.')

    l1_ts = os.path.dirname(l1).rstrip('/') + '/timestamps.pkl'
    l2_ts = os.path.dirname(l2).rstrip('/') + '/timestamps.pkl'

    for fp in [l1_ts, l2_ts]:
        if not os.path.exists(fp):
            raise ValueError(f'Required file {fp!r} does not exist.')

    to = to.lower()
    if to != 'csv' and to != 'gtfs':
        raise ValueError(
            f'"to" must be one of "csv" or "gtfs", but the value {to!r} was provided instead.'
        )

    with open(l1_ts, 'rb') as fp:
        l1_ts = pickle.load(fp)
    with open(l2_ts, 'rb') as fp:
        l2_ts = pickle.load(fp)

    _, l1_ext = os.path.splitext(l1)
    _, l2_ext = os.path.splitext(l2)

    if l1_ext.lower() == 'csv':
        l1 = gt.ops.from_csv(l1)
    else:  # l1_ext.lower() == 'gtfs'
        raise NotImplementedError
    
    if l2_ext.lower() == 'csv':
        l2 = gt.ops.from_csv(l2)
    else:  # l2_ext.lower() == 'gtfs'
        raise NotImplementedError

    l, l_ts = gt.ops.merge_logbooks([(l1, l1_ts), (l2, l2_ts)])

    if not no_clean:
        l = gt.ops.discard_partial_logs(l)
        l = gt.ops.cut_cancellations(l)

    if to == 'csv':
        gt.ops.to_csv(l, outpath)
    else:  # to == 'gtfs'
        gt.ops.to_gtfs(l, outpath)

    if include_timestamp_log:
        with open(os.path.dirname(outpath).rstrip('/') + '/timestamps.pkl', 'wb') as fp:
            pickle.dump(l_ts, fp)

    if include_error_log:
        raise NotImplementedError

cli.add_command(logify)
cli.add_command(merge)
