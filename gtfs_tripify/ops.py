"""
Operations defined on logbooks.
"""

import warnings
import itertools
from collections import defaultdict
from datetime import datetime, timedelta
import pytz

import numpy as np
import pandas as pd
from google.transit import gtfs_realtime_pb2

from gtfs_tripify.utils import synthesize_route, finish_trip


##############
# HEURISTICS #
##############

def cut_cancellations(logbook):
    """
    Heuristically cuts stops that almost certainly didn't happen do to trip cancellations. I 
    refer to this as the "cut-cancellation" heuristic.

    Returns a minified logbook containing only trips that almost assuredly happened.
    """
    def cut_cancellations_log(log):
        # Immediately return if the log is empty.
        if len(log) == 0:
            return log
        # Heuristically return an empty log if there are zero confirmed stops in the log.
        elif (~(log.action == "STOPPED_AT").any() and 
              len(log.latest_information_time.unique())) == 1:
            return log.head(0)
        else:
            # Find the last definite stop.
            last_definite_stop = (log.latest_information_time ==\
                log.latest_information_time.unique()[-1]).idxmax() - 1
            # Heuristically cut len >= 2 `STOPPED_OR_SKIPPED` blocks with the same 
            # `LATEST_INFORMATION_TIME`.
            suspicious_block = log.tail(-last_definite_stop - 1)
            if len(suspicious_block) == 1:
                return log
            elif len(suspicious_block['latest_information_time'].unique()) == 1:
                return log.head(last_definite_stop + 1)
            else:
                return log

    for unique_trip_id in list(logbook.keys()):
        updated_log = cut_cancellations_log(logbook[unique_trip_id])
        if len(updated_log) > 0:
            logbook[unique_trip_id] = updated_log
        else:
            del logbook[unique_trip_id]  # remove now-empty trips

    return logbook


def discard_partial_logs(logbook):
    """
    Discards logs which appear in the first or last message in the feed. These logs are extremely
    likely to be partial because we do not get to "see" every single message corresponding with 
    the trip, as some are outside our "viewing window".
    """
    trim = logbook.copy()

    times = np.array(
        list(
            itertools.chain(
                *[logbook[trip_id]['latest_information_time'].values for trip_id in logbook.keys()]
            )
        )
    ).astype(int)
    first, last = np.min(times), np.max(times)

    for trip_id in logbook.keys():
        if logbook[trip_id]['latest_information_time'].astype(int).isin([first, last]).any():
            trim.pop(trip_id)

    return trim


def drop_invalid_messages(update):
    """
    Given a feed update (as returned by `dictify`), catch certain non-fatal errors that, though 
    they violate the GTFS-RT spec, are still valid in the Protobuf spec. These are operator errors
    made by the feed publisher. A warning is raised and the non-conformant messages are dropped.
    """
    parse_errors = []
    fixed_update = {'header': update['header'], 'entity': []}
    trip_ids_to_drop = set()
    trip_ids_seen = []
    messages_to_drop_idxs = set()
    trip_update_ids = set()
    vehicle_update_ids = set()

    # Capture and throw away messages which (1) null trip_id values or (2) empty stop sequences.
    for idx, message in enumerate(update['entity']):
        if message['type'] == 'vehicle_update':
            message_type = 'vehicle_update'
            message_trip_id = message['vehicle']['trip']['trip_id']
            vehicle_update_ids.add(message_trip_id)
        else:  # message['type'] == 'trip_update'
            message_type = 'trip_update'
            message_trip_id = message['trip_update']['trip']['trip_id']
            number_of_stops_remaining = len(message['trip_update']['stop_time_update'])
            trip_update_ids.add(message_trip_id)

        if message_trip_id == '':
            messages_to_drop_idxs.add(idx)
            warnings.warn(
                f"The message at the {idx} position in the GTFS-RT update for "
                f"{update['header']['timestamp']} has a null trip id. This invalid "
                f"message was removed from the update during pre-processing."
            )
            parse_errors.append({
                'type': 'message_with_null_trip_id',
                'details': {
                    'update_timestamp': update['header']['timestamp'],
                    'message_index': idx,
                    'message_body': message
                }
            })
        elif message_type == 'trip_update' and number_of_stops_remaining == 0:
            messages_to_drop_idxs.add(idx)
            warnings.warn(
                f"The trip with the ID {message_trip_id} was provided a trip update with "
                f"no stops in it in the GTFS-RT update for {update['header']['timestamp']}. "
                f"The messages correspondong with this invalid trip were removed from the "
                f"update during pre-processing."
            )
            trip_ids_to_drop.add(message_trip_id)
            parse_errors.append({
                'type': 'trip_has_trip_update_with_no_stops_remaining',
                'details': {
                    'update_timestamp': update['header']['timestamp'],
                    'message_index': idx,
                    'message_body': message
                }
            })
            if message_trip_id in trip_ids_seen:
                complimentary_message_to_drop_idx = trip_ids_seen.index(message_trip_id)
                messages_to_drop_idxs.add(complimentary_message_to_drop_idx)
        elif message_trip_id in trip_ids_to_drop:
            messages_to_drop_idxs.add(idx)

        trip_ids_seen.append(message_trip_id)

    # Capture and throw away vehicle updates that do not also have trip updates.
    # Note that this can result in multiple validation errors against a single message.
    trip_update_only_ids = trip_update_ids.difference(vehicle_update_ids)
    for trip_update_only_id in trip_update_only_ids:
        warnings.warn(
            f"The trip with ID {trip_update_only_id} is provided a vehicle update but no "
            f"trip update in the GTFS-R update for {update['header']['timestamp']}. "
            f"This invalid trip was removed from the update during pre-processing."
        )
        parse_errors.append({
            'type': 'trip_id_with_trip_update_but_no_vehicle_update',
            'details': {
                'trip_id': trip_update_only_id,
                'update_timestamp': update['header']['timestamp']
            }
        })
        messages_to_drop_idxs.add(trip_update_only_id)

    for idx, message in enumerate(update['entity']):
        if idx not in messages_to_drop_idxs:
            fixed_update['entity'].append(message)

    return fixed_update, parse_errors


def drop_duplicate_messages(updates):
    """
    Given a list of feed updates, drops updates from the feed which appear more than once 
    (have the same timestamp). This would occur when the feed returns stale data.
    """
    # TODO: warn on non-sequential timestamps
    # TODO: warn on non-sensical and/or empty timestamps
    parse_errors = []

    if updates == []:
        return [], []

    ts_prior = updates[0]['header']['timestamp']
    out = [updates[0]]

    for idx in range(1, len(updates)):
        ts_curr = updates[idx]['header']['timestamp']
        if ts_curr == ts_prior:
            warnings.warn(
                f"There are multiple messages in the GTFS-R update for timestamp "
                f"{updates[idx]['header']['timestamp']}. The duplicate updates were removed "
                f"during pre-processing."
            )
            parse_errors.append({
                'type': 'feed_updates_with_duplicate_timestamps',
                'details': {
                    'update_timestamp': updates[idx]['header']['timestamp'],
                    'update_index': idx,
                    'message_body': updates[idx]
                }
            })
        else:
            out.append(updates[idx])
        ts_prior = ts_curr

    return out, parse_errors


# TODO: implement, test, inject into logify logic
def drop_nonsequential_messages(updates):
    """
    Given a list of feed updates, drop updates from the feed with timestamps that don't make
    sense. E.g. a timestamp of 0, or empty string '', or a timestamp that is otherwise out of
    sequence from its neighbors.
    """
    pass


def partition_on_incomplete(logbook, timestamps):
    """
    Partitions a logbook (and associated timestamps) into two parts: a logbook with complete
    logs, and a logbook with log records. A log is incomplete if there is at least one station
    outstanding, e.g. at least one station that the train is still EN_ROUTE_TO.
    """
    complete_logbook = pd.DataFrame(columns=logbook.columns)
    complete_timestamps = dict()
    incomplete_logbook = pd.DataFrame(columns=logbook.columns)
    incomplete_timestamps = dict()

    for unique_trip_id in logbook:
        log = logbook[unique_trip_id]
        if logbook[unique_trip_id].action.iloc[-1] == 'EN_ROUTE_TO':
            incomplete_logbook[unique_trip_id] = log
            complete_timestamps[unique_trip_id] = timestamps[unique_trip_id]
        else:
            complete_logbook[unique_trip_id] = log
            incomplete_timestamps[unique_trip_id] = timestamps[unique_trip_id]
    
    return complete_logbook, complete_timestamps, incomplete_logbook, incomplete_timestamps


def partition_on_route_id(logbook, timestamps):
    """
    Partitions a logbook (and associated timestamps) into multiple separate logbooks based
    on the route_id. This is useful for I/O; when you write to disk it makes sense to organize
    your files based on route.
    """
    route_logbooks, route_timestamps = defaultdict(dict), defaultdict(dict)

    for unique_trip_id in logbook:
        log = logbook[unique_trip_id]
        route_id = log.route_id.iloc[0]
        route_logbooks[route_id][unique_trip_id] = log
        route_timestamps[route_id][unique_trip_id] = timestamps[unique_trip_id]

    return route_logbooks, route_timestamps


####################
# MERGING LOGBOOKS #
####################

# This code painfully duplicates a lot of logic in tripify.py, but it would be difficult
# to write something logical (from a UX perspective) otherwise.
def merge_logbooks(logbook_tuples):
    """
    Given a list of trip logbook data in the form [(logbook, logbook_timestamps), ...] in 
    time-sort order, perform a merge.
    """
    left = dict()
    left_timestamps = dict()
    for (right, right_timestamps) in logbook_tuples:
        left, left_timestamps = join_logbooks(left, left_timestamps, right, right_timestamps)
    return left, left_timestamps


def join_logbooks(left, left_timestamps, right, right_timestamps):
    """
    Given two trip logbooks and their associated timestamps, get their merger.
    """
    # Trivial cases.
    if len(right) == 0:
        return left, left_timestamps
    if len(left) == 0:
        return right, right_timestamps

    # TODO: attempt to reroot trips that cancel in between logbooks instead of always cancelling
    # There are five kinds of joins that we care about (but see the above).
    # (1) complete trips on the left side, just append
    # (2) complete trips on the right side, just append
    # (3) incomplete trips on the left side that do not appear on the right, which we interpret
    #     as cancellations. A future improvement would be to
    # (4) incomplete trips on the left side that do appear on the right, these are joiners
    # (5) incomplete trips on the right side that do not appear on the left, just append
    incomplete_trips_left = [unique_trip_id for unique_trip_id in left\
        if left[unique_trip_id].action.iloc[-1] == 'EN_ROUTE_TO']
    left_map = {left[unique_trip_id].trip_id.iloc[0]: unique_trip_id for
                unique_trip_id in incomplete_trips_left}
    right_map = {left[unique_trip_id].trip_id.iloc[0]: None for 
                 unique_trip_id in incomplete_trips_left}
    first_right_timestamp = np.min([*(itertools.chain(right_timestamps.values()))])

    # determine candidate right trips based on trip_id match
    # pick the one which appears in the first timestamp included in the right time slice
    # and run _join_trip_logs on that matched object
    # if no such trip exists, this is a cancellation, so perform the requisite work

    for unique_trip_id_right in right:
        right_trip = right[unique_trip_id_right]
        trip_id = right_trip.trip_id.iloc[0]

        # if there is no match we can just append
        if trip_id not in left_map:
            left[unique_trip_id_right] = right_trip
            left_timestamps[unique_trip_id_right] = right_timestamps[unique_trip_id_right]

        # if there is a match we need to do more work
        elif (trip_id in left_map and
              right_timestamps[unique_trip_id_right][0] == first_right_timestamp):
            assert right_map[trip_id] is None
            right_map[trip_id] = right[unique_trip_id_right]

    for trip_id in right_map:
        trip_data_right = right_map[trip_id]
        unique_trip_id_left = left_map[trip_id]

        # for trips we found a match for, perform the merge
        if trip_data_right is not None:
            left[left_map[trip_id]] = _join_trip_logs(
                left[unique_trip_id_left], trip_data_right
            )
            left_timestamps[unique_trip_id_left] =\
                left_timestamps[unique_trip_id_left] + right_timestamps[unique_trip_id_right]
            del left_map[trip_id]

        # for trips we did not find a match for, finalize as a cancellation
        else:
            left[unique_trip_id_left] = finish_trip(left[unique_trip_id_left], first_right_timestamp)


    # for trips we did not find a a match for
    # finalize trips that were incomplete in the left and also didn't appear in the right
    # this is whatever's left that's in the left_map after joins are done
    for trip_id in left_map:
        unique_trip_id = left_map[trip_id]
        left[unique_trip_id] = finish_trip(left[unique_trip_id], first_right_timestamp)

    return left, left_timestamps


def _join_trip_logs(left, right):
    """
    Two trip logs may contain information based on action logs, and GTFS-Realtime feed updates, 
    which are discontiguous in time. In other words, these logs reflect the same trip, but are 
    based on different sets of observations.

    In such cases recovering a full(er) record requires merging these two logs together. Here we 
    implement this operation.
    """
    # Order the frames so that the earlier one is on the left.
    left_start = left['latest_information_time'].min()
    right_start = right['latest_information_time'].min()
    if right_start < left_start:
        left, right = right, left

    # Get the combined synthetic station list.
    stations = synthesize_route([list(left['stop_id'].values), list(right['stop_id'].values)])
    right_stations = set(right['stop_id'].values)

    # Combine the station information in last-precedent order.
    l_i = 0
    left_indices = []

    for station in stations:
        if station not in right_stations:
            left_indices.append(l_i)
            l_i += 1

    # Combine records.
    join = pd.concat([left.iloc[left_indices], right]).reset_index(drop=True)

    # Declaring an ordinal categorical column in the stop_id attribute makes `pandas` handle 
    # resorting internally and, hence, results in a significant speedup (over doing so ourselves).
    join['stop_id'] = pd.Categorical(join['stop_id'], stations, ordered=True)

    # Update records for stations before the first station in the right trip log that the train
    # is EN_ROUTE_TO or STOPPED_OR_SKIPPED.
    swap_station = right.iloc[0]['stop_id']
    swap_index = next(i for i, station in enumerate(stations) if station == swap_station)
    swap_space = join[:swap_index]
    where_update = swap_space[swap_space['action'] == 'EN_ROUTE_TO'].index.values

    join.loc[where_update, 'action'] = 'STOPPED_OR_SKIPPED'
    join.loc[where_update, 'maximum_time'] = right.loc[0, 'latest_information_time']
    join.loc[swap_index, 'minimum_time'] = left.loc[0, 'minimum_time']

    # Hard-case the columns to float so as to avoid weird typing issues that keep coming up.
    join.loc[:, ['minimum_time', 'maximum_time']] =\
        join.loc[:, ['minimum_time', 'maximum_time']].astype(float)

    # The second trip update may on the first index contain incomplete minimum time information
    # due to not having a reference to a previous trip update included in that trip log's 
    # generative action log set. There are a number of ways in which this can occur, but the end
    # fact of the matter is that between the last entry in the first trip log and the first entry
    # in the second trip log, we may have one of three different inconsistencies:
    #
    # 1. The prior states that the train stopped at (or skipped) the last station in that log at
    #    some known time, but the minimum time of the first stop or skip in the posterior log is
    #    a NaN, due to lack of prior information.
    # 2. The prior states that the train stopped at (or skipped) the last station in that log at
    #    some known minimum time, but the posterior log first entry minimum time is even earlier.
    # 3. The prior states that the train stopped at (or skipped) the last station in that log at
    #    some known maximum time, but the posterior log first entry minimum time is even earlier.
    #
    # The lines below handle each one of these possible inconsistencies in turn.
    join.loc[:, 'minimum_time'] = join.loc[:, 'minimum_time'].fillna(method='ffill')
    join.loc[1:, 'minimum_time'] = np.maximum.accumulate(join.loc[1:, 'minimum_time'].values)

    if len(join) > 1:
        join.loc[len(left) -1, 'minimum_time'] = np.maximum(
            np.nan_to_num(join.loc[len(left) - 2, 'maximum_time']),
            join.loc[len(left) - 1, 'minimum_time']
        )

    # Again at the location of the join, we may also get an incomplete `maximum_time` entry, 
    # for the same reason. In this case we will take the `maximum_time` of the following entry. 
    # However, note that we are *losing information* in this case, as we could technically 
    # resolve this time to a more accurate one, given the full list of information times. 
    # However, we do not have that information at this time in the processing sequence. This is
    # an unfortunate but not particularly important, all things considered, technical 
    # shortcoming.
    
    join.loc[:, 'maximum_time'] = join.loc[:, 'maximum_time'].fillna(method='bfill', limit=1)

    join = join.assign(latest_information_time=join.latest_information_time.astype(int))
    return join


#######
# I/O #
#######

def parse_feed(bytes):
    """
    Helper function for reading a feed in using Protobuf. 
    Handles bad feeds by replacing them with None.
    """
    # TODO: tests.
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        try:
            fm = gtfs_realtime_pb2.FeedMessage()
            fm.ParseFromString(bytes)
            return fm

        # Protobuf occasionally raises an unexpected tag RuntimeWarning. This occurs when a
        # feed that we read has unexpected problems, but is still valid overall. This 
        # warning corresponds with data loss in most cases. `gtfs-tripify` is sensitive to the
        # disappearance of trips in the record. If data is lost, it's best to excise the 
        # message entirely. Hence we catch these warnings and return a flag value None, to be
        # taken into account upstream. For further information see the following thread:
        # https://groups.google.com/forum/#!msg/mtadeveloperresources/9Fb4SLkxBmE/BlmaHWbfw6kJ
        except RuntimeWarning:
            warnings.warn(
                f"The Protobuf parser raised a RunTimeWarning while parsing an update, indicating "
                f"possible corruption and/or loss of data. This update cannot be safely used "
                f"upstream and has been dropped."
            )
            return None

        # Raise for all other errors.
        except:
            raise


def to_gtfs(logbook, filename, tz=None, output=False):
    """
    Write a logbook into a GTFS stops.txt record. Some important things to keep in mind when 
    using this method:

    * If there is no known minimum_time for a stop, a time 15 seconds before the maximum_time
      will be imputed. GTFS does not allow for null values.
    * If there is no known maximum time for a stop, the stop will not be included in the file.
    * If the train is still en route to a stop, that stop will not be included in the file.

    It's recommended you only use to_gtfs on complete logbooks.
    """
    rows = []
    tz = tz if tz is not None else pytz.timezone("US/Eastern")
    for unique_trip_id in logbook:
        log = logbook[unique_trip_id]
        for idx, srs in log.iterrows():
            if srs['action'] == 'EN_ROUTE_TO':
                warnings.warn(
                    f'{unique_trip_id} contains stops which the train is still EN_ROUTE_TO, '
                    f'which were not written to the output for lack of completeness. It is '
                    f'recommended to only run to_gtfs on complete logbooks.'
                )
            elif pd.isnull(srs['maximum_time']):
                warnings.warn(
                    f'{unique_trip_id} contains stops with no known departure time. These '
                    f'stops were not written to the output for lack of completeness. It is '
                    f'recommended to only run to_gtfs on complete logbooks.'
                )
            else:
                if pd.notnull(srs['minimum_time']): 
                    arrival_timestamp = srs['minimum_time']
                else:
                    warnings.warn(
                        f'{unique_trip_id} contains stops with no known arrival time. These '
                        f'stops will be assigned an inferred arrival time of 15 seconds prior '
                        f'to their departure time. It is recommended to only run to_gtfs on '
                        f'complete logbooks.'
                    )
                    arrival_timestamp = (
                        datetime.utcfromtimestamp(srs['maximum_time']) 
                        - timedelta(seconds=15)
                    ).timestamp()
                departure_timestamp = srs['maximum_time']
                arrival_time = tz.localize(
                    datetime.utcfromtimestamp(arrival_timestamp), is_dst=None
                ).strftime('%H:%M:%S')
                departure_time = tz.localize(
                    datetime.utcfromtimestamp(departure_timestamp), is_dst=None
                ).strftime('%H:%M:%S')
                rows.append(
                    [
                        unique_trip_id, 
                        arrival_time,
                        departure_time,
                        srs['stop_id'],
                        idx + 1, 0, 0
                    ]
                )

    out = pd.DataFrame(
        rows,
        columns=[
            'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 
            'pickup_type', 'drop_off_type'
        ]
    )

    if output:
        return out
    else:
        out.to_csv(filename, index=False)


def to_csv(logbook, filename, output=False):
    """
    Write a logbook to a CSV file.
    """
    logs = []
    for unique_trip_id in logbook:
        log = logbook[unique_trip_id].assign(unique_trip_id=unique_trip_id)
        logs.append(log)

    if output:
        return pd.concat(logs)
    else:
        return pd.concat(logs).to_csv(filename, index=False)


def from_csv(filename):
    """
    Read a logbook from a CSV file.
    """
    g = pd.read_csv(filename).groupby('unique_trip_id')
    return {k: df.drop(columns='unique_trip_id') for k, df in g}


__all__ = [
    'cut_cancellations', 'discard_partial_logs', 'drop_invalid_messages', 
    'drop_duplicate_messages', 'partition_on_incomplete', 'merge_logbooks', 'parse_feed'
]
