"""
`gtfs-tripify` core test module. Asserts that all data generation steps are correct.
"""
import unittest
import collections

import numpy as np
import pandas as pd
from google.transit import gtfs_realtime_pb2
import pytest

import gtfs_tripify as gt
from gtfs_tripify.tripify import (
    dictify, actionify, logify, tripify, drop_invalid_messages, collate
)
from gtfs_tripify.ops import join_logbooks, drop_nonsequential_messages


# some of these tests use ./fixtures/gtfs-* fixtures.
# you can recreate these fixtures yourself from archival data by running the following:
# import gtfs_tripify as gt; import datetime
# messages, dates =\
#      gt.utils.load_mytransit_archived_feeds(timestamp=datetime.datetime(2016, 5, 12, 4, 0))
# addtl_fixtures = [(message, name) for (message, name) in zip(messages, names) if (
#   'gtfs-20160512T04' in name or 'gtfs-20160512T0500Z' in name
# )]
# for message, name in addtl_fixtures: 
#     with open('fixtures/' + name, 'wb') as fp: 
#         fp.write(message.read())

class TestDictify(unittest.TestCase):
    def setUp(self):
        with open("./fixtures/gtfs-20160512T0400Z", "rb") as f:
            gtfs = gtfs_realtime_pb2.FeedMessage()
            gtfs.ParseFromString(f.read())

        self.gtfs = gtfs

    def test_dictify(self):
        feed = dictify(self.gtfs)
        assert set(feed) == {'entity', 'header'}
        assert feed['header'].keys() == {'timestamp', 'gtfs_realtime_version'}
        assert isinstance(feed['header']['timestamp'], int)
        assert(dict(collections.Counter([message['type'] for message in feed['entity']]))) ==\
            {'alert': 1, 'trip_update': 94, 'vehicle_update': 68}
        assert len(feed['entity']) == 1 + 94 + 68

        assert feed['entity'][-1]['type'] == 'alert'
        assert set(feed['entity'][-1]['alert']['informed_entity'][0].keys()) ==\
            {'route_id', 'trip_id'}

        assert feed['entity'][-2]['type'] == 'trip_update'
        assert len(feed['entity'][-2]['trip_update']['stop_time_update']) == 2
        assert set(feed['entity'][-2]['trip_update']['stop_time_update'][0].keys()) ==\
            {'arrival', 'departure', 'stop_id'}

        assert feed['entity'][5]['type'] == 'vehicle_update'
        assert set(feed['entity'][5]['vehicle'].keys()) ==\
            {'trip', 'stop_id', 'timestamp', 'current_stop_sequence', 'current_status'}


class TestActionify(unittest.TestCase):
    def test_case_1(self):
        """
        The train is STOPPED_AT a station somewhere along the route. The train is not expected 
        to skip any of the stops along the route.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': 1463026170, 'departure': 1463026170, 'stop_id': '104S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'STOPPED_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == [
            'STOPPED_AT', 'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT', 
            'EXPECTED_TO_ARRIVE_AT'
        ]

    def test_case_2(self):
        """
        The train is currently IN_TRANSIT_TO a station somewhere along the route. The train is 
        not expected to skip any of the stops along the route.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': 1463026170, 'departure': 1463026170, 'stop_id': '104S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'IN_TRANSIT_TO',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == [
            'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_ARRIVE_AT',
            'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_ARRIVE_AT'
        ]

    def test_case_3(self):
        """
        The train is currently INCOMING_AT a station somewhere along the route. This case is 
        treated the same was as the case above.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': 1463026170, 'departure': 1463026170, 'stop_id': '104S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == [
            'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_ARRIVE_AT',
            'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_ARRIVE_AT'
        ]

    def test_case_4(self):
        """
        The train is queued. The train is not expected to skip any of the stops along the route.

        Every station except for the first and last has an EXPECTED_TO_ARRIVE_AT and
        EXPECTED_TO_DEPART_AT entry. The last only has an EXPECTED_TO_ARRIVE_AT entry. 
        The first only has an EXPECTED_TO_DEPART_AT entry.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': np.nan, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': 1463026170, 'departure': 1463026170, 'stop_id': '104S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, None, timestamp)
        assert list(log['action']) == [
            'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT',
            'EXPECTED_TO_ARRIVE_AT'
        ]

    def test_case_5(self):
        """
        The train is currently INCOMING_AT the final stop on its route.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '140S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == ['EXPECTED_TO_ARRIVE_AT']

    def test_case_6(self):
        """
        The train is currently IN_TRANSIT_TO the final stop on its route.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'IN_TRANSIT_TO',
                'current_stop_sequence': 34,
                'stop_id': '140S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == ['EXPECTED_TO_ARRIVE_AT']

    def test_case_7(self):
        """
        The train is currently STOPPED_AT the final stop on its route.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'STOPPED_AT',
                'current_stop_sequence': 34,
                'stop_id': '140S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == ['STOPPED_AT']

    def test_case_8(self):
        """
        The train is somewhere along its trip, and follows all of the same rules as the similar
        earlier test cases thereof. However, it is also expected to skip one or more stops 
        along its route.

        There are actually two such cases. In the first case, we have an intermediate station
        with only a departure. In the second, an intermediate station with only an arrival.

        One hopes that there isn't some special meaning attached to the difference between the
        two.
        """
        # First subcase.
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': 1463026170, 'departure': np.nan, 'stop_id': '104S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == [
            'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_SKIP',
            'EXPECTED_TO_ARRIVE_AT'
        ]

        # Second subcase.
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': np.nan, 'departure': 1463026170, 'stop_id': '104S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == [
            'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT', 'EXPECTED_TO_SKIP',
            'EXPECTED_TO_ARRIVE_AT'
        ]

    def test_case_9(self):
        """
        The train is expected to skip the first stop along its route.
        """
        # First subcase.
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': np.nan, 'departure': 1463026080, 'stop_id': '103S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == ['EXPECTED_TO_SKIP', 'EXPECTED_TO_ARRIVE_AT']

        # Second subcase.
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': '000650_1..S02R'},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': np.nan, 'stop_id': '103S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': '137100_1..N02X017'
                }
            }
        }
        timestamp = 1463025417

        log = actionify(trip_message, vehicle_message, timestamp)
        assert list(log['action']) == ['EXPECTED_TO_SKIP', 'EXPECTED_TO_ARRIVE_AT']


class TestCorrectFeed(unittest.TestCase):
    def test_vehicle_update_only(self):
        """
        Assert that we raise a warning and remove the entry with `correct` when a trip 
        only have a vehicle warning in the feed.
        """
        trip_message = {
            'id': '000001',
            'type': 'trip_update',
            'trip_update': {
                'trip': {'route_id': '1',
                         'start_date': '20160512',
                         'trip_id': ''},
                'stop_time_update': [
                    {'arrival': 1463026080, 'departure': np.nan, 'stop_id': '103S'},
                    {'arrival': 1463029500, 'departure': np.nan, 'stop_id': '140S'}
                ]
            }
        }
        vehicle_message = {
            'id': '000006',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': ''
                }
            }
        }
        feed = {
            'header': {'gtfs_realtime_version': 1,
                       'timestamp': 1463025417},
            'entity': [trip_message, vehicle_message]
        }

        with pytest.warns(UserWarning):
            feed, parse_errors = drop_invalid_messages(feed)

        assert len(feed['entity']) == 0
        assert len(parse_errors) == 2

    def test_empty_trip_id(self):
        """
        Assert that we raise a warning and remove the entry when a feed entity has a null trip_id.
        """
        vehicle_message = {
            'id': '',
            'type': 'vehicle_update',
            'vehicle': {
                'current_status': 'INCOMING_AT',
                'current_stop_sequence': 34,
                'stop_id': '103S',
                'timestamp': 1463025417,
                'trip': {
                    'route_id': '1',
                    'start_date': '20160511',
                    'trip_id': ''
                }
            }
        }
        feed = {
            'header': {'gtfs_realtime_version': 1,
                       'timestamp': 1463025417},
            'entity': [vehicle_message]
        }

        with pytest.warns(UserWarning):
            feed, parse_errors = drop_invalid_messages(feed)

        assert len(feed['entity']) == 0
        assert len(parse_errors) == 1

    def test_empty_trip_id_2(self):
        feed = {'header': {'timestamp': 1},
                'entity': [{'id': '000022',
                            'type': 'vehicle_update',
                            'vehicle': {'current_stop_sequence': 0,
                                        'stop_id': '',
                                        'current_status': 'IN_TRANSIT_TO',
                                        'timestamp': 0,
                                        'trip': {'route_id': '1',
                                                 'trip_id': '',
                                                 'start_date': '20160511'}}}]}
        with pytest.warns(UserWarning):
            feed, parse_errors = drop_invalid_messages(feed)

        assert len(feed['entity']) == 0
        assert len(parse_errors) == 1

    def test_trip_message_with_no_stops(self):
        """
        Assert that we raise a warning and remove offending entries when a feed entity
        has a trip update with no stops in it.
        """
        feed = {
            'header': {'timestamp': 1},
            'entity': [
                {
                    'id': '000022',
                    'type': 'vehicle_update',
                    'vehicle': {
                        'current_stop_sequence': 0,
                        'stop_id': '100N',
                        'current_status': 'IN_TRANSIT_TO',
                        'timestamp': 1,
                        'trip': {
                            'route_id': 'A',
                            'trip_id': 'A_B_C',
                            'start_date': '20160512'
                        }
                    }
                },
                {
                    'id': '000023',
                    'type': 'trip_update',
                    'trip_update': {
                        'trip': {
                            'route_id': 'A',
                            'trip_id': 'A_B_C',
                            'start_date': '20160512'
                        },
                        'stop_time_update': []
                    }
                }
            ]
        }
        with pytest.warns(UserWarning):
            feed, parse_errors = drop_invalid_messages(feed)

        assert len(feed['entity']) == 0
        assert len(parse_errors) == 1


class DropInvalidUpdateTimestampsTests(unittest.TestCase):
    """
    Tests for dropping feed updates with bad timestamps.
    """
    def test_drop_update_timestamps_empty(self):
        stream = []
        stream, parse_errors = drop_nonsequential_messages(stream)

        assert len(stream) == 0
        assert len(parse_errors) == 0

    def test_update_sequence_going_backwards_in_time(self):
        update_1 = {
            'header': {'timestamp': 1},
            'entity': []
        }
        update_2 = {
            'header': {'timestamp': 0},
            'entity': []
        }
        stream = [update_1, update_2]

        with pytest.warns(UserWarning):
            stream, parse_errors = drop_nonsequential_messages(stream)

        assert len(stream) == 1
        assert stream[0]['header']['timestamp'] == 1
        assert len(parse_errors) == 1

    def test_update_sequence_null_empty_string_timestamp(self):
        update = {'header': {'timestamp': ''}}
        stream = [update]

        with pytest.warns(UserWarning):
            stream, parse_errors = drop_nonsequential_messages(stream)

        assert len(stream) == 0
        assert len(parse_errors) == 1

    def test_update_sequence_null_zero_timestamp(self):
        update = {'header': {'timestamp': '0'}}
        stream = [update]

        with pytest.warns(UserWarning):
            stream, parse_errors = drop_nonsequential_messages(stream)

        assert len(stream) == 0
        assert len(parse_errors) == 1

        update = {'header': {'timestamp': 0}}
        stream = [update]

        with pytest.warns(UserWarning):
            stream, parse_errors = drop_nonsequential_messages(stream)

        assert len(stream) == 0
        assert len(parse_errors) == 1


def create_mock_action_log(actions=None, stops=None, information_time=0, trip_id=None):
    length = len(actions)
    return pd.DataFrame({
        'trip_id': ['TEST'] * length if trip_id is None else [trip_id] * length,
        'route_id': [1] * length,
        'action': actions,
        'stop_id': ['999X'] * length if stops is None else stops,
        'information_time': [information_time] * length,
        'time_assigned': list(range(length))
    })


class TripLogUnaryTests(unittest.TestCase):
    """
    Tests for simpler cases which can be processed in a single action log.
    """

    def test_unary_stopped(self):
        """
        An action log with just a stoppage ought to report as a trip log with just a stoppage.
        """
        actions = [create_mock_action_log(['STOPPED_AT'])]
        result, _ = tripify(actions)

        assert len(result) == 1
        assert result.iloc[0].action == 'STOPPED_AT'
        assert all([
            str(result.iloc[0]['maximum_time']) == 'nan',
            str(result.iloc[0]['minimum_time']) == 'nan',
            int(result.iloc[0]['latest_information_time']) == 0]
        )

    def test_unary_en_route(self):
        """
        An action log with just an arrival and departure ought to report as just an arrival 
        (note: this is the same technically broken case tested by the eight test in action log 
        testing; for more on when this arises, check the docstring there).
        """
        actions = [
            create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT'])
        ]
        result, _ = tripify(actions)

        assert len(result) == 1
        assert result.iloc[0].action == 'EN_ROUTE_TO'
        assert all([result.iloc[0]['maximum_time'] in [np.nan, 'nan'],
                    int(result.iloc[0]['minimum_time']) == 0,
                    int(result.iloc[0]['latest_information_time']) == 0])

    def test_unary_end(self):
        """
        An action log with a single arrival ought to report a single EN_ROUTE_TO in the trip log.
        """
        result, _ = tripify([
            create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT'])
        ])
        assert len(result) == 1
        assert result.iloc[0].action == 'EN_ROUTE_TO'
        assert all([result.iloc[0]['maximum_time'] in [np.nan, 'nan'],
                    int(result.iloc[0]['minimum_time']) == 0,
                    int(result.iloc[0]['latest_information_time']) == 0])

    def test_unary_arriving_skip(self):
        """
        An action log with a stop to be skipped ought to report an arrival at that stop in the 
        resultant trip log. This is because we leave the job of detecting a skip to the 
        combination process.

        This is an "arriving skip" because a skip will occur on a station that has either a 
        departure or arrival defined, but not both.
        """
        actions = [
            create_mock_action_log(
                actions=[
                    'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART',
                    'EXPECTED_TO_ARRIVE_AT'
                ],
                stops=['999X', '998X', '998X', '997X'])
        ]
        result, _ = tripify(actions)

        assert len(result) == 3
        assert list(result['action'].values) == ['EN_ROUTE_TO', 'EN_ROUTE_TO', 'EN_ROUTE_TO']
        assert list(result['minimum_time'].values.astype(int)) == [0] * 3
        assert list(result['maximum_time'].values.astype(str)) == ['nan'] * 3

    def test_unary_departing_skip(self):
        """
        An action log with a stop to be skipped ought to report an arrival at that stop in the 
        resultant trip log. This is because we leave the job of detecting a skip to the 
        combination process.

        This is an "arriving skip" because a skip will occur on a station that has either a 
        departure or arrival defined, but not both.
        """
        actions = [
            create_mock_action_log(
                actions=[
                    'EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART', 'EXPECTED_TO_DEPART',
                    'EXPECTED_TO_ARRIVE_AT'
                ], 
                stops=['999X', '998X', '998X', '997X']
            )
        ]
        result, _ = tripify(actions)

        assert len(result) == 3
        assert list(result['action'].values) == ['EN_ROUTE_TO', 'EN_ROUTE_TO', 'EN_ROUTE_TO']
        assert result['action'].values.all() == 'EN_ROUTE_TO'
        assert list(result['minimum_time'].values.astype(int)) == [0] * 3
        assert list(result['maximum_time'].values.astype(str)) == ['nan'] * 3

    def test_unary_en_route_trip(self):
        """
        A slightly longer test. Like `test_unary_en_route`, but with a proper station terminus.
        """
        actions = [
            create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_FROM',
                                            'EXPECTED_TO_ARRIVE_AT'],
                                   stops=['999X', '999X', '998X'])
        ]
        result, _ = tripify(actions)

        assert len(result) == 2
        assert list(result['action'].values) == ['EN_ROUTE_TO', 'EN_ROUTE_TO']
        assert result['action'].values.all() == 'EN_ROUTE_TO'
        assert list(result['minimum_time'].values.astype(int)) == [0] * 2
        assert list(result['maximum_time'].values.astype(str)) == ['nan'] * 2

    def test_unary_ordinary_stopped_trip(self):
        """
        A slightly longer test. Like `test_unary_stopped`, but with an additional arrival 
        after the present one.
        """
        actions = [
            create_mock_action_log(actions=['STOPPED_AT', 'EXPECTED_TO_ARRIVE_AT'],
                                   stops=['999X', '998X'])
        ]
        result, _ = tripify(actions)

        assert len(result) == 2
        assert list(result['action'].values) == ['STOPPED_AT', 'EN_ROUTE_TO']
        assert list(result['minimum_time'].values.astype(str)) == ['nan', '0']
        assert list(result['maximum_time'].values.astype(str)) == ['nan', 'nan']


class TripLogBinaryTests(unittest.TestCase):
    """
    Tests for more complicated cases necessitating both action logs.

    These tests do not invoke station list changes between action logs, e.g. they do not 
    address reroutes.
    """

    def test_binary_en_route(self):
        """
        In the first observation, the train is EN_ROUTE to a station. In the next observation, 
        it is still EN_ROUTE to that station.

        Our output should be a trip log with a single row. Critically, the minimum_time 
        recorded should correspond with the time at which the second observation was made---1, 
        in this test case.
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_ARRIVE_AT'],
                                      stops=['999X', '999X'])
        first = base.head(1)
        second = base.assign(information_time=base.information_time.values[:-1].tolist() + [1])
        actions = [first, second]

        result, _ = tripify(actions)

        assert len(result) == 1
        assert list(result['action'].values) == ['EN_ROUTE_TO']
        assert list(result['minimum_time'].values.astype(int)) == [1]
        assert list(result['maximum_time'].values.astype(str)) == ['nan']

    def test_binary_en_route_stop(self):
        """
        In the first observation, the train is EN_ROUTE to a station. In the next observation, 
        it STOPPED_AT that station.

        Our output should be a trip log with a single row, recording the time at which the stop 
        was made.
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT', 'STOPPED_AT'],
                                      stops=['999X', '999X'])
        first = base.head(1)
        second = base.assign(information_time=base.information_time.values[:-1].tolist() + [1])
        actions = [first, second]

        result, _ = tripify(actions)

        assert len(result) == 1
        assert list(result['action'].values) == ['STOPPED_AT']
        assert list(result['minimum_time'].values.astype(int)) == [0]
        assert list(result['maximum_time'].values.astype(str)) == ['nan']

    def test_binary_stop_or_skip_en_route(self):
        """
        In the first observation, the train is EN_ROUTE to a station. In the next observation, 
        it is EN_ROUTE to a different station, one further along in the record.

        Our output should be a trip log with two rows, one STOPPED_OR_SKIPPED at the first 
        station, and one EXPECTED_TO_ARRIVE_AT in another.
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_DEPART_AT',
                                               'EXPECTED_TO_ARRIVE_AT',
                                               'EXPECTED_TO_ARRIVE_AT'],
                                      stops=['999X', '999X', '998X', '998X'])
        first = base.head(3)
        second = base.assign(information_time=base.information_time.values[:-1].tolist() + [1])
        actions = [first, second]

        result, _ = tripify(actions)

        assert len(result) == 2
        assert list(result['action'].values) == ['STOPPED_OR_SKIPPED', 'EN_ROUTE_TO']
        assert list(result['minimum_time'].values.astype(int)) == [0, 1]
        assert list(result['maximum_time'].values.astype(str)) == ['1', 'nan']

    def test_binary_skip_en_route(self):
        """
        In the first observation, the train is EN_ROUTE to a station which it is going to skip. 
        In the second observation the train is en route to another station further down the line.

        Our output should be a trip log with two rows. The first entry should be a 
        STOPPED_OR_SKIPPED at the first station, and then the second should be an EN_ROUTE_TO 
        at the second station.
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_SKIP', 'EXPECTED_TO_ARRIVE_AT',
                                               'EXPECTED_TO_ARRIVE_AT'],
                                      stops=['999X', '998X', '998X'])
        first = base.head(2)
        second = base.assign(information_time=base.information_time.values[:-1].tolist() + [1])
        actions = [first, second]

        result, _ = tripify(actions)

        assert len(result) == 2
        assert list(result['action'].values) == ['STOPPED_OR_SKIPPED', 'EN_ROUTE_TO']
        assert list(result['minimum_time'].values.astype(int)) == [0, 1]
        assert list(result['maximum_time'].values.astype(str)) == ['1', 'nan']

    def test_binary_skip_stop(self):
        """
        In the first observation, the train is EN_ROUTE to a station which it is going to skip. 
        In the second observation the train is stopped at another station further down the line.

        Our output should be a trip log with two rows. The first entry should be a 
        STOPPED_OR_SKIPPED at the first station, and then the second should be a STOPPED_AT at 
        the second station.
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_SKIP', 'EXPECTED_TO_ARRIVE_AT',
                                               'STOPPED_AT'],
                                      stops=['999X', '998X', '998X'])
        first = base.head(2)
        second = base.assign(information_time=base.information_time.values[:-1].tolist() + [1])
        actions = [first, second]

        result, _ = tripify(actions)

        assert len(result) == 2
        assert list(result['action'].values) == ['STOPPED_OR_SKIPPED', 'STOPPED_AT']
        assert list(result['minimum_time'].values.astype(int)) == [0, 0]
        assert list(result['maximum_time'].values.astype(str)) == ['1', 'nan']


class TripLogReroutingTests(unittest.TestCase):
    """
    These tests make sure that trip logs work as expected when the train gets rerouted.
    """
    def test_en_route_reroute(self):
        """
        Behavior when en route, and rerouted to another en route, is that the earlier station(s) 
        ought to be marked "STOPPED_OR_SKIPPED".
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT', 'EXPECTED_TO_ARRIVE_AT'],
                                      stops=['999X', '998X'])
        first = base.head(1)
        second = base.assign(information_time=base.information_time.values[:-1].tolist() + [1])

        result, _ = tripify([first, second])

        assert len(result) == 2
        assert list(result['action'].values) == ['STOPPED_OR_SKIPPED', 'EN_ROUTE_TO']


class TripLogFinalizationTests(unittest.TestCase):
    """
    Tests for finalization.

    Within the GTFS-Realtime log, a signal that a train trip is complete only comes in the 
    form of that trip's messages no longer appearing in the queue in the next update. The 
    most recently recorded message may be in any conceivable state prior to this occurring. 
    Finalization is the procedure "capping off" any still to-be-arrived-at stations. Since this 
    involves contextual knowledge about records appearing and not appearing in the data stream,
    this procedure is provided as a separate method.

    These tests ascertain that said method, `_finish_trip`, works as advertised.
    """
    def test_finalize_no_op(self):
        """
        Finalization should do nothing to trip logs that are already complete.
        """
        base = create_mock_action_log(actions=['STOPPED_AT'], stops=['999X'])
        result, _ = tripify([base], finished=True, finish_information_time=np.nan)

        assert len(result) == 1
        assert list(result['action'].values) == ['STOPPED_AT']

    def test_finalize_en_route(self):
        """
        Finalization should cap off trips that are still EN_ROUTE.
        """
        base = create_mock_action_log(actions=['EXPECTED_TO_ARRIVE_AT'], stops=['999X'])
        result, _ = tripify([base], finished=True, finish_information_time=np.nan)

        assert len(result) == 1
        assert list(result['action'].values) == ['STOPPED_OR_SKIPPED']

    def test_finalize_all(self):
        """
        Make sure that finalization works across columns as well.
        """
        base = create_mock_action_log(
            actions=['EXPECTED_TO_SKIP', 'EXPECTED_TO_ARRIVE_AT'], 
            stops=['999X', '998X']
        )
        first = base.head(1)
        second = base.tail(1)
        result, _ = tripify([first, second], finished=True, finish_information_time=42)

        assert len(result) == 2
        assert list(result['action'].values) == ['STOPPED_OR_SKIPPED', 'STOPPED_OR_SKIPPED']
        assert list(result['maximum_time'].astype(int).values) == [42, 42]


class LogbookTests(unittest.TestCase):
    """
    Smoke tests for generating trip logbooks and merging them.
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

    def test_logbook(self):
        logbook, _, _ = logify([self.log_0, self.log_1])
        assert len(logbook) == 94


class LogbookJoinTests(unittest.TestCase):
    """
    These tests make sure that the logbook join logic is correct.
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

    def test_logbook_join(self):
        left, left_timestamps, _ = logify([self.log_0])
        right, right_timestamps, _ = logify([self.log_1])

        result, result_timestamps = join_logbooks(left, left_timestamps, right, right_timestamps)
        assert len(result) == 94
        assert result.keys() == left.keys()  # only true in this simple case
        assert (result[list(result.keys())[0]].head(1) !=
                left[list(result.keys())[0]].head(1)).any().any()
        assert len(result_timestamps) == 94

    def test_trivial_join(self):
        """In the trivial case one or the other or both logbooks are actually empty."""
        information_time = 0
        actions = create_mock_action_log(
            actions=['STOPPED_AT', 'STOPPED_AT'],
            information_time=information_time
        )
        trip = tripify([actions])[0]
        logbook = {'uuid': trip}  # as would be returned by gt.logify
        timestamps = {'uuid': [information_time]}

        # empty right and nonempty left
        empty_logbook, empty_timestamps = pd.DataFrame(columns=trip.columns), dict()
        result, _ = join_logbooks(logbook, timestamps, empty_logbook, empty_timestamps)
        assert result.keys() == logbook.keys()

        # empty left and nonempty right
        result, _ = join_logbooks(empty_logbook, empty_timestamps, logbook, timestamps)
        assert result.keys() == logbook.keys()

        # both empty
        result, _ = join_logbooks(
            empty_logbook, empty_timestamps, empty_logbook, empty_timestamps
        )
        assert len(result) == 0

    def test_only_complete_trips(self):
        """
        The simplest non-trivial case: trips on either side are complete and just get merged in.
        """
        actions_1 = create_mock_action_log(
            actions=['STOPPED_AT'], information_time=1, trip_id='TRIP_1'
        )
        actions_2 = create_mock_action_log(
            actions=['STOPPED_AT'], information_time=2, trip_id='TRIP_2'
        )
        left_logbook = {'uuid1': tripify([actions_1])[0]}
        left_timestamps = {'uuid1': [1]}
        right_logbook = {'uuid2': tripify([actions_2])[0]}
        right_timestamps = {'uuid2': [2]}

        result, result_timestamps =\
            join_logbooks(left_logbook, left_timestamps, right_logbook, right_timestamps)
        assert list(result.keys()) == ['uuid1', 'uuid2']
        assert (all(result['uuid1'].trip_id == 'TRIP_1') and 
                all(result['uuid2'].trip_id == 'TRIP_2'))
        assert len(result_timestamps.keys()) == 2

    def test_incomplete_completable_trips(self):
        """
        There is a trip on the left that is incomplete, but completeable (or at least extendable)
        using information on the right.
        """
        actions_1 = create_mock_action_log(
            actions=['EN_ROUTE_TO', 'EN_ROUTE_TO'], information_time=1, stops=['500X', '501X']
        )
        actions_2 = create_mock_action_log(
            actions=['STOPPED_AT'], information_time=2, stops=['501X']
        )
        left_logbook = {'uuid1': tripify([actions_1])[0]}
        left_timestamps = {'uuid1': [1]}
        right_logbook = {'uuid2': tripify([actions_2])[0]}
        right_timestamps = {'uuid2': [2]}

        # left log is en-route to two stops, right log is stopped at second of the two stops
        # expect logic to correctly mark first station STOPPED_OR_SKIPPED and correctly mark
        # second station STOPPED_AT
        result, result_timestamps =\
            join_logbooks(left_logbook, left_timestamps, right_logbook, right_timestamps)
        assert len(result) == 1
        assert result_timestamps['uuid1'] == [1, 2]
        assert result['uuid1'].action.values.tolist() == ['STOPPED_OR_SKIPPED', 'STOPPED_AT']

    def test_incomplete_uncompletable_trip(self):
        """
        There is a trip on the left that is incomplete, but no new information is offered on
        the right.
        """
        actions = create_mock_action_log(actions=['EN_ROUTE_TO'], information_time=1)
        log = tripify([actions])[0]
        left_logbook, right_logbook = {'uuid1': log}, {'uuid2': log}
        left_timestamps, right_timestamps = {'uuid1': [1]}, {'uuid2': [2]}

        result, result_timestamps =\
            join_logbooks(left_logbook, left_timestamps, right_logbook, right_timestamps)
        
        assert len(result) == 1
        # TODO: latest_information_time should change, and it should match the newer value,
        # currently that's not what happens. This is a bug that needs to be fixed.
        # assert result['uuid1'].latest_information_time.values.tolist() == [2]
        assert result['uuid1'].action.values.tolist() == ['EN_ROUTE_TO']
        assert result_timestamps['uuid1'] == [1, 2]

    def test_incomplete_cancelled_trip(self):
        """
        There is a trip on the left that is incomplete and which is not matched to any trips
        on the right. This indicates a cancellation in the gap between the two logbooks.
        """
        actions1 = create_mock_action_log(
            actions=['EN_ROUTE_TO'], information_time=1, trip_id='A'
        )
        trip1 = tripify([actions1])[0]
        left_logbook, left_timestamps = {'uuid1': trip1}, {'uuid1': [1]}

        actions2 = create_mock_action_log(
            actions=['STOPPED_AT'], information_time=2, trip_id='B'
        )
        trip2 = tripify([actions2])[0]
        right_logbook, right_timestamps = {'uuid2': trip2}, {'uuid2': [2]}

        result, _ = join_logbooks(
            left_logbook, left_timestamps, right_logbook, right_timestamps
        )
        assert result['uuid1'].action.values.tolist() == ['STOPPED_OR_SKIPPED']


def create_mock_update_feed(
    stop_time_update, trip_id=None, route_id=None, timestamp=None, current_status=None,
    current_stop_id=None, include_vehicle_message=True
):
    trip_message = {
        'id': '000001',
        'type': 'trip_update',
        'trip_update': {
            'trip': {'route_id': '1' if not route_id else route_id,
                     'start_date': '20160512',
                     'trip_id': '1' if not trip_id else trip_id},
            'stop_time_update': stop_time_update
        }
    }
    vehicle_message = {
        'id': '000002',
        'type': 'vehicle_update',
        'vehicle': {
            'current_status': 'STOPPED_AT' if current_status is None else current_status,
            'current_stop_sequence': 1,
            'stop_id': current_stop_id if current_stop_id else '102S',
            'timestamp': 1463025417 if not timestamp else timestamp,
            'trip': {
                'route_id': '1' if not route_id else route_id,
                'start_date': '20160512',
                'trip_id': '1' if not trip_id else trip_id
            }
        }
    }
    return {
        'header': {
            'gtfs_realtime_version': 1, 'timestamp': 1463025417 if not timestamp else timestamp
        },
        'entity': [trip_message, vehicle_message] if include_vehicle_message else [trip_message]
    }


class LogbookTripMergeTests(unittest.TestCase):
    """
    End-to-end runs are not unique on trip_id and may be broken up into several trip segments.
    These tests ascertain that the heuristic we are using for detecting and concatenating the
    subset of such segments that are actually a single trip works as expected.
    """

    def test_logbook_trip_merge_trip_id_changed(self):
        """
        A merge where only the trip_id has changed.
        """
        stop_seq = [{'arrival': 1463026080, 'departure': np.nan, 'stop_id': '103S'}]
        feed_1 = create_mock_update_feed(stop_seq, trip_id='1', timestamp=1463025417)
        feed_2 = create_mock_update_feed(stop_seq, trip_id='2', timestamp=1463025418)

        result, _, _ = logify([feed_1, feed_2])
        assert len(result) == 1

    def test_logbook_trip_merge_trip_id_and_incoming_status_changed(self):
        """
        A merge where the trip_id and incoming status has changed.
        """
        stop_seq = [{'arrival': 1463026080, 'departure': np.nan, 'stop_id': '103S'}]
        feed_1 = create_mock_update_feed(
            stop_seq, trip_id='1', timestamp=1463025417, current_status='STOPPED_AT',
            current_stop_id='102S'
        )
        feed_2 = create_mock_update_feed(
            stop_seq, trip_id='2', timestamp=1463025418, current_status='INCOMING_AT',
            current_stop_id='103S'
        )

        result, _, _ = logify([feed_1, feed_2])
        assert len(result) == 1

    def test_logbook_trip_merge_threeway_id_changed(self):
        """
        A merge where the trip got segments more than once.
        """
        stop_seq = [{'arrival': 1463026080, 'departure': np.nan, 'stop_id': '103S'}]
        feed_1 = create_mock_update_feed(
            stop_seq, trip_id='1', timestamp=1463025417, current_status='STOPPED_AT',
            current_stop_id='102S'
        )
        feed_2 = create_mock_update_feed(
            stop_seq, trip_id='2', timestamp=1463025418, current_status='STOPPED_AT',
            current_stop_id='102S'
        )
        feed_3 = create_mock_update_feed(
            stop_seq, trip_id='3', timestamp=1463025419, current_status='STOPPED_AT',
            current_stop_id='102S'
        )

        result, _, _ = logify([feed_1, feed_2, feed_3])
        assert len(result) == 1

    def test_logbook_trip_merge_threeway_trip_id_and_incoming_status_changed(self):
        """
        A merge where the trip got segments more than once, and also the incoming status changes.
        """
        stop_seq = [{'arrival': 1463026080, 'departure': np.nan, 'stop_id': '103S'}]
        feed_1 = create_mock_update_feed(
            stop_seq, trip_id='1', timestamp=1463025417, current_status='INCOMING_AT',
            current_stop_id='102S'
        )
        feed_2 = create_mock_update_feed(
            stop_seq, trip_id='2', timestamp=1463025418, current_status='STOPPED_AT',
            current_stop_id='102S'
        )
        feed_3 = create_mock_update_feed(
            stop_seq, trip_id='3', timestamp=1463025419, current_status='STOPPED_AT',
            current_stop_id='102S'
        )

        result, _, _ = logify([feed_1, feed_2, feed_3])
        assert len(result) == 1
