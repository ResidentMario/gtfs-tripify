"""
`gtfs-tripify` IO test module. Asserts that IO functions are correct.
"""
import unittest
import sqlite3

import pandas as pd

import gtfs_tripify as gt
from gtfs_tripify.ops import logbook_to_sql, stream_to_sql, discard_partial_logs

class TestLogbookToSQL(unittest.TestCase):
    """
    Tests the logbook SQL writer utility.
    """
    def setUp(self):
        self.log_columns = ['trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                            'latest_information_time']

    def testTableInitialization(self):
        """
        Test that a table is initialized correctly, if no preexisting table exists.
        """
        conn = sqlite3.connect(":memory:")
        logbook_to_sql({}, conn)

        c = conn.cursor()
        result = c.execute("SELECT COUNT(*) FROM Logbooks").fetchone()
        assert result == (0,)

        c.close()
        conn.close()

    def testDuplicatedKeyInsertion(self):
        """
        If two logbooks with duplicated trip keys are inserted one after the other, remap the colliding key.
        """
        conn = sqlite3.connect(":memory:")

        log = pd.DataFrame(columns=self.log_columns,
                           data=[
                               ['same_trip_id', '_', '_', '_', '_', '_', '_']
                           ])

        logbook = {'same_trip_id_0': log}

        logbook_to_sql(logbook, conn)
        logbook_to_sql(logbook, conn)

        c = conn.cursor()
        result = c.execute("SELECT DISTINCT unique_trip_id FROM Logbooks").fetchall()
        assert result == [('same_trip_id_0',), ('same_trip_id_1',)]

        c.close()
        conn.close()

    def testDoublyDuplicatedKeyInsertion(self):
        """
        Suppose two logbooks with duplicated trip_ids are inserted one after the other. Furthermore, suppose that
        the logbooks contain multiple logbook_trip_ids corresponding with the trip_id in question. That is to say
        that during the log generation process, we discovered two distinct trips with the same trip number,
        so we indexed the resulting logs with keys that have _0 and _1 appended, respectively.

        An illustrative example:
        logbook1 = {'this_trip_id_0': <...>, ..., 'this_trip_id_1': <...>, 'this_trip_id_2': <...>}
        logbook2 = {'this_trip_id_0': <...>, ..., 'this_trip_id_1': <...>}

        This test asserts that the database write will do the correct thing when processing these feeds, by writing
        them to the database with the keys {'this_trip_id_0', ..., 'this_trip_id_4'} (or whatever else is apropos).

        If two logbooks with duplicated trip keys are inserted one after the other, remap the colliding key.
        """
        conn = sqlite3.connect(":memory:")

        log = pd.DataFrame(columns=self.log_columns,
                           data=[
                               ['same_trip_id', '_', '_', '_', '_', '_', '_'],
                           ])

        logbook = {'same_trip_id_0': log, 'same_trip_id_1': log}

        logbook_to_sql(logbook, conn)
        logbook_to_sql(logbook, conn)

        c = conn.cursor()
        result = set(c.execute("SELECT DISTINCT unique_trip_id FROM Logbooks").fetchall())
        assert result == {('same_trip_id_0',), ('same_trip_id_1',), ('same_trip_id_2',), ('same_trip_id_3',)}

        c.close()
        conn.close()

    def testDoublyDuplicatedKeyInsertionVariant(self):
        """
        Like the above, but with logbooks of different lengths.
        """
        conn = sqlite3.connect(":memory:")

        log = pd.DataFrame(columns=self.log_columns,
                           data=[
                               ['same_trip_id', '_', '_', '_', '_', '_', '_'],
                           ])

        logbook1 = {'same_trip_id_0': log, 'same_trip_id_1': log}
        logbook2 = {'same_trip_id_0': log, 'same_trip_id_1': log, 'same_trip_id_2': log}

        logbook_to_sql(logbook1, conn)
        logbook_to_sql(logbook2, conn)

        c = conn.cursor()
        result = set(c.execute("SELECT DISTINCT unique_trip_id FROM Logbooks").fetchall())
        assert result == {('same_trip_id_0',), ('same_trip_id_1',), ('same_trip_id_2',), ('same_trip_id_3',),
                          ('same_trip_id_4',)}

        c.close()
        conn.close()


class TestStreamToSQL(unittest.TestCase):
    """
    Tests the stream SQL writer utility. This method is a thin wrapper, the logic is tested elsewhere.
    """
    def setUp(self):
        self.stream = ["./fixtures/gtfs-20160512T0400Z", "./fixtures/gtfs-20160512T0401Z"]

    def testWithoutParser(self):
        """
        The method works as expected without a parser.
        """
        conn = sqlite3.connect(":memory:")
        stream_to_sql(self.stream, conn)
        c = conn.cursor()

        result = c.execute("SELECT COUNT(*) FROM Logbooks").fetchone()
        assert result == (2079,)

        c.close()
        conn.close()

    def testWithNullTransform(self):
        """
        The method works as expected with an identity transform.
        """
        conn = sqlite3.connect(":memory:")
        stream_to_sql(self.stream, conn, transform=lambda logbook: logbook)
        c = conn.cursor()

        result = c.execute("SELECT COUNT(*) FROM Logbooks").fetchone()
        assert result == (2079,)

        c.close()
        conn.close()

    def testWithNonNullTransform(self):
        """
        The method works as expected with a real transform.
        """
        conn = sqlite3.connect(":memory:")
        stream_to_sql(self.stream, conn, transform=lambda logbook: discard_partial_logs(logbook))
        c = conn.cursor()

        result = c.execute("SELECT COUNT(*) FROM Logbooks").fetchone()
        assert result == (0,)  # all logs retrieved from a two-message stream are partial, so all are discarded.

        c.close()
        conn.close()
