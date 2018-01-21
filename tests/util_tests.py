"""
`gtfs-tripify` utilities test module. Asserts that utility functions are correct.
"""

import unittest
import pandas as pd
import sqlite3

import sys; sys.path.append("../")
import gtfs_tripify as gt


class TestCutCancellations(unittest.TestCase):
    """
    Tests the cut-cancellation heuristic.
    """
    def setUp(self):
        self.log_columns = ['trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                            'latest_information_time']

    def test_no_op(self):
        """
        The heuristic should do nothing if the log is empty.
        """
        log = pd.DataFrame(columns=self.log_columns)
        result = gt.utils.cut_cancellations(log)
        assert len(result) == 0

    def test_zero_confirmed(self):
        """
        The heuristic should return an empty log if there are zero confirmed stops in the log.
        """
        log = pd.DataFrame(columns=self.log_columns, data=[['_', '_', 'STOPPED_OR_SKIPPED', '_', '_', '_', '_']])
        result = gt.utils.cut_cancellations(log)
        assert len(result) == 0

    def test_zero_tailing_unconfirmed(self):
        """
        The heuristic should return an unmodified log if there are no tailing `STOPPED_OR_SKIPPED` records.
        """
        log = pd.DataFrame(columns=self.log_columns, data=[['_', '_', 'STOPPED_AT', '_', '_', '_', '_']])
        result = gt.utils.cut_cancellations(log)
        assert len(result) == 1

    def test_one_tailing_unconfirmed(self):
        """
        The heuristic should return an unmodified log if there is one tailing `STOPPED_OR_SKIPPED` record.
        """
        log = pd.DataFrame(columns=self.log_columns,
                           data=[
                               ['_', '_', 'STOPPED_AT', '_', '_', '_', '_'],
                               ['_', '_', 'STOPPED_OR_SKIPPED', '_', '_', '_', '_']
                           ])
        result = gt.utils.cut_cancellations(log)
        assert len(result) == 2

    def test_many_unique_tailing_unconfirmed(self):
        """
        The heuristic should return an unmodified log if there is at least one `STOPPED_AT` record and many
        tailing `STOPPED_OR_SKIPPED` records, but the logs have two or more unique `LATEST_INFORMATION_TIME` values.
        """
        log = pd.DataFrame(columns=self.log_columns,
                           data=[
                               ['_', '_', 'STOPPED_AT', '_', '_', '_', 0],
                               ['_', '_', 'STOPPED_OR_SKIPPED', '_', '_', '_', 0],
                               ['_', '_', 'STOPPED_OR_SKIPPED', '_', '_', '_', 1]
                           ])
        result = gt.utils.cut_cancellations(log)
        assert len(result) == 3

    def test_many_nonunique_tailing_unconfirmed(self):
        """
        The heuristic should return a block-cleaned log if there is at least one `STOPPED_AT` record and many tailing
        `STOPPED_OR_SKIPPED` records, but the logs have just one unique `LATEST_INFORMATION_TIME` values.
        """
        log = pd.DataFrame(columns=self.log_columns,
                           data=[
                               ['_', '_', 'STOPPED_AT', '_', '_', '_', 0],
                               ['_', '_', 'STOPPED_OR_SKIPPED', '_', '_', '_', 1],
                               ['_', '_', 'STOPPED_OR_SKIPPED', '_', '_', '_', 1]
                           ])
        result = gt.utils.cut_cancellations(log)
        assert len(result) == 1


class TestDiscardPartialLogs(unittest.TestCase):
    """
    Tests the partial log heuristic.
    """
    def setUp(self):
        self.log_columns = ['trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                            'latest_information_time']

    def test_single_discard(self):
        """
        If there's just one record matching the first-or-last `LATEST_INFORMATION_TIME` condition, discard that one.
        """
        first = pd.DataFrame(columns=self.log_columns,
                             data=[
                                 ['_', '_', '_', '_', '_', '_', 0],
                                 ['_', '_', '_', '_', '_', '_', 2]
                             ])
        second = pd.DataFrame(columns=self.log_columns,
                             data=[
                                 ['_', '_', '_', '_', '_', '_', 1]
                             ])
        logbook = {'_0': first, '_1': second}
        result = gt.utils.discard_partial_logs(logbook)
        assert len(result) == 1

    def test_multiple_discard(self):
        """
        If there's more than one record matching the first-or-last `LATEST_INFORMATION_TIME` condition, discard them
        all.
        """
        first = pd.DataFrame(columns=self.log_columns,
                             data=[
                                 ['_', '_', '_', '_', '_', '_', 0],
                                 ['_', '_', '_', '_', '_', '_', 1]
                             ])
        second = pd.DataFrame(columns=self.log_columns,
                              data=[
                                   ['_', '_', '_', '_', '_', '_', 1]
                              ])
        third = pd.DataFrame(columns=self.log_columns,
                             data=[
                                 ['_', '_', '_', '_', '_', '_', 0],
                                 ['_', '_', '_', '_', '_', '_', 2]
                             ])

        logbook = {'_0': first, '_1': second, '_2': third}
        result = gt.utils.discard_partial_logs(logbook)
        assert len(result) == 1


class TestToSQL(unittest.TestCase):
    """
    Tests the SQL writer utility.
    """
    def setUp(self):
        self.log_columns = ['trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                            'latest_information_time']

    def testTableInitialization(self):
        """
        Test that a table is initialized correctly, if no preexisting table exists.
        """
        conn = sqlite3.connect(":memory:")
        gt.utils.to_sql({}, conn)

        c = conn.cursor()
        result = c.execute("SELECT COUNT(*) FROM LOGBOOKS").fetchone()
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

        gt.utils.to_sql(logbook, conn)
        gt.utils.to_sql(logbook, conn)

        c = conn.cursor()
        result = c.execute("SELECT DISTINCT unique_trip_id FROM LOGBOOKS").fetchall()
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

        gt.utils.to_sql(logbook, conn)
        gt.utils.to_sql(logbook, conn)

        c = conn.cursor()
        result = c.execute("SELECT DISTINCT unique_trip_id FROM LOGBOOKS").fetchall()
        assert result == [('same_trip_id_0',), ('same_trip_id_1',), ('same_trip_id_2',), ('same_trip_id_3',)]

        c.close()
        conn.close()

    def testDoublyDuplicatedKeyInsertion(self):
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

        gt.utils.to_sql(logbook1, conn)
        gt.utils.to_sql(logbook2, conn)

        c = conn.cursor()
        result = set(c.execute("SELECT DISTINCT unique_trip_id FROM LOGBOOKS").fetchall())
        assert result == {('same_trip_id_0',), ('same_trip_id_1',), ('same_trip_id_2',), ('same_trip_id_3',),
                          ('same_trip_id_4',)}

        c.close()
        conn.close()
