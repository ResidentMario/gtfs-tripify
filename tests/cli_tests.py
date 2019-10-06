import unittest
import os
import warnings
from click.testing import CliRunner

from gtfs_tripify import cli as gtfs_cli


class TestLogify(unittest.TestCase):
    def test_invalid(self):
        cli = CliRunner()

        # nonexistent file or directory
        assert cli.invoke(gtfs_cli.logify, ['fixtures/', 'dne/dne']).exit_code != 0
        assert cli.invoke(gtfs_cli.logify, ['nonexistent', 'stops.csv']).exit_code != 0

        # to is set to something invalid
        result = cli.invoke(gtfs_cli.logify, [__file__, os.getcwd(), '--to', 'badformat'])
        assert result.exit_code != 0

    def test_valid(self):
        cli = CliRunner()
        fixtures_fp = os.getcwd().rstrip('/') + '/fixtures/'

        with cli.isolated_filesystem():
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                cli.invoke(
                    gtfs_cli.logify, [fixtures_fp, 'stops.csv', '--no-clean', '--to', 'csv']
                )
                assert os.path.exists(os.getcwd().rstrip('/') + '/stops.csv')

        with cli.isolated_filesystem():
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                cli.invoke(
                    gtfs_cli.logify, [fixtures_fp, 'stops.txt', '--no-clean', '--to', 'gtfs']
                )
                assert os.path.exists(os.getcwd().rstrip('/') + '/stops.txt')
