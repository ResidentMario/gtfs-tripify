import click
# import os
# import itertools
# from pathlib import Path

# from pkg_resources import iter_entry_points


@click.group()
def cli():
    pass


@click.command()
@click.argument('input_dir')
@click.argument('outpath')
@click.option('--no-clean', help='Disable cleaning up the logbook before writing it to disk.')
@click.option('--include-timestamp-log',
              help='Write the timestamp log to disk. Timestamps are used for merging logbooks.')
@click.option('--include-error-log', help='Write the error log to disk.')
@click.option(
    '--to', default='gtfs', show_default=True, help='The output format. Must be "csv" or "gtfs".'
)
def logify():
    # TODO: implement
    # click.echo('Initialized the database')
    pass


@click.command()
@click.argument('logbook_fp_1')
@click.argument('logbook_fp_2')
@click.argument('outpath')
@click.option('--no-clean', help='Disable cleaning up the logbook before writing it to disk.')
@click.option(
    '--to', default='gtfs', show_default=True, help='The output format. Must be "csv" or "gtfs".'
)
def merge_logbooks():
    # TODO: implement
    pass


cli.add_command(logify)
cli.add_command(merge_logbooks)
