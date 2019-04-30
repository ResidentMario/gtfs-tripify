"""
`gtfs-tripify` IO test module. Asserts that IO functions are correct.
"""
import unittest
import sqlite3
import warnings
import os

import pandas as pd
from google.transit import gtfs_realtime_pb2

from gtfs_tripify.tripify import logify, dictify
from gtfs_tripify.ops import to_gtfs, to_csv, from_csv

class TestLogbooksToGTFS(unittest.TestCase):
    """
    Test the logbook GTFS writer utility.
    """
    def setUp(self):
        with open("./fixtures/gtfs-20160512T0400Z", "rb") as f:
            gtfs_0 = gtfs_realtime_pb2.FeedMessage()
            gtfs_0.ParseFromString(f.read())

        with open("./fixtures/gtfs-20160512T0401Z", "rb") as f:
            gtfs_1 = gtfs_realtime_pb2.FeedMessage()
            gtfs_1.ParseFromString(f.read())

        self.log_0 = dictify(gtfs_0)
        self.log_1 = dictify(gtfs_1)
        self.logbook, _ = logify([self.log_0, self.log_1])

        for unique_trip_id in self.logbook:
            self.logbook[unique_trip_id].maximum_time = 1556588500

    def test_to_gtfs_smoke(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # throws >100 warnings ;)
            gtfs = to_gtfs(self.logbook, None, output=True)
        assert len(gtfs) > 0


class TestLogbooksToCSV(unittest.TestCase):
    """
    Test the logbook CSV writer utility.
    """
    def setUp(self):
        with open("./fixtures/gtfs-20160512T0400Z", "rb") as f:
            gtfs_0 = gtfs_realtime_pb2.FeedMessage()
            gtfs_0.ParseFromString(f.read())

        with open("./fixtures/gtfs-20160512T0401Z", "rb") as f:
            gtfs_1 = gtfs_realtime_pb2.FeedMessage()
            gtfs_1.ParseFromString(f.read())

        self.log_0 = dictify(gtfs_0)
        self.log_1 = dictify(gtfs_1)
        self.logbook, _ = logify([self.log_0, self.log_1])

    def test_to_csv_roundtrip(self):
        to_csv(self.logbook, 'temp.csv')
        result = from_csv('temp.csv')

        assert result.keys() == self.logbook.keys()
        example_uid = list(result.keys())[0]
        assert result[example_uid].equals(self.logbook[example_uid])
        os.remove('temp.csv')
