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
    Removes reassigned stops from a logbook. Example usage:

    .. code:: python

        import gtfs_tripify as gt
        import requests

        response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
        response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
        response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
        stream = [response1.content, response2.content, response3.content]

        logbook, timestamps, parse_errors = gt.logify(stream)
        gt.ops.cut_cancellations(logbook)

    GTFS-Realtime messages from certain transit providers suffer from trip fragmentation:
    trains may be reassigned IDs and schedules mid-trip. ``gtfs_tripify`` naively assumes that
    trips that disappeared from the record in this way completed all of their remaining scheduled
    stops, even though they didn't.
    
    This method removes those such stops in a logbook which almost assuredly did not happen using
    a best-effort heuristic. ``cut_cancellations`` is robust if and only if transitioning from the
    second-to-last stop to the last stop on the route takes more than ``$TIME_INTERVAL`` seconds,
    where ``$TIME_INTERVAL`` is the distance between feed messages.
    
    If this constraint is violated, either because the interval between the last two stops in the
    service is unusually short, or due to downtime in the underlying feed, some data will be
    unfixably ambiguous and may be lost.
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
    Removes partial logs from a logbook. Example usage:

    .. code:: python

        import gtfs_tripify as gt
        import requests

        response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
        response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
        response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
        stream = [response1.content, response2.content, response3.content]

        logbook, timestamps, parse_errors = gt.logify(stream)
        logbook = gt.ops.discard_partial_logs(logbook)

    Logbooks are constructed on a "time slice" of data. Trips that appear in the first or last
    message included in the time slice are necessarily incomplete. These incomplete trips may be:

    * Left as-is.
    * Completed by merging this logbook with a time-contiguous one (using
      ``gt.ops.merge_logbooks``)
    * Partitioned out (using ``gt.ops.partition_on_incomplete``).
    * Pruned from the logbook (using this method).

    The best course of action is dependent on your use case.
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
        elif message['type'] == 'trip_update':
            message_type = 'trip_update'
            message_trip_id = message['trip_update']['trip']['trip_id']
            number_of_stops_remaining = len(message['trip_update']['stop_time_update'])
            trip_update_ids.add(message_trip_id)
        else:  # message['type'] == 'alert'
            continue

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
                f"The messages corresponding with this invalid trip were removed from the "
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
    parse_errors = []

    if updates == []:
        return [], []

    ts_prior = updates[0]['header']['timestamp']
    out = [updates[0]]

    for idx in range(1, len(updates)):
        ts_curr = updates[idx]['header']['timestamp']
        if ts_curr == ts_prior:
            warnings.warn(
                f"There are multiple messages in the GTFS-RT update for timestamp "
                f"{updates[idx]['header']['timestamp']}. The duplicate updates were removed "
                f"during pre-processing."
            )
            parse_errors.append({
                'type': 'feed_updates_with_duplicate_timestamps',
                'details': {
                    'update_timestamp': updates[idx]['header']['timestamp'],
                    'message_index': idx,
                    'message_body': updates[idx]
                }
            })
        else:
            out.append(updates[idx])
        ts_prior = ts_curr

    return out, parse_errors


def drop_nonsequential_messages(updates):
    """
    Given a list of feed updates, drop updates from the feed with timestamps that don't make
    sense. E.g. a timestamp of 0, or empty string '', or a timestamp that is otherwise out of
    sequence from its neighbors.
    """
    timestamps = [update['header']['timestamp'] for update in updates]
    update_idxs_to_remove = set()
    parse_errors = []

    for ts_idx, ts in enumerate(timestamps):
        if ts == 0 or ts == '0' or ts == '':
            warnings.warn(
                f"The GTFS-RT update at position {ts_idx} in the stream has an erronous "
                f"timestamp value of \"{ts}\". This update has been removed from the stream "
                f"during pre-processing."
            )
            parse_errors.append({
                'type': 'feed_update_has_null_timestamp',
                'details': {
                    'update_timestamp': timestamps[ts_idx],
                    'update_index': ts_idx
                }
            })
            update_idxs_to_remove.add(ts_idx)
        elif ts_idx > 0:
            ts_prior = timestamps[ts_idx - 1]
            if ts_prior > ts:
                warnings.warn(
                    f"The GTFS-RT update at position {ts_idx} in the stream is for timestamp "
                    f"{ts}, but the GTFS-RT update at position {ts_idx - 1} is for timestamp "
                    f"{ts_prior}. This is an error in the feed as it should be impossible to "
                    f"go backwards. This update has been removed from the stream during "
                    f"pre-processing."
                )
                parse_errors.append({
                    'type': 'feed_update_goes_backwards_in_time',
                    'details': {
                        'update_index': ts_idx,
                        'update_timestamp': timestamps[ts_idx],
                        'prior_timestamp': timestamps[ts_idx - 1]
                    }
                })
                update_idxs_to_remove.add(ts_idx)
    
    out = []
    for update_idx, update in enumerate(updates):
        if update_idx not in update_idxs_to_remove:
            out.append(update)
    
    return out, parse_errors


def partition_on_incomplete(logbook, timestamps):
    """
    Partitions incomplete logs in a logbook into a separate logbook. Incomplete logs are logs
    in the logbook for trips that were already in progress as of the first feed update included in
    the parsed messages, or were still in progress as of the last feed update included in the
    parsed messages.
    
    This operation is useful when merging logbooks. See also ``gt.ops.discard_partial_logs``.
    Example usage:

    .. code:: python

        import gtfs_tripify as gt
        import requests

        response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
        response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
        response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
        stream = [response1.content, response2.content, response3.content]
        logbook, timestamps, parse_errors = gt.logify(stream)

        complete_logbook, complete_timestamps, incomplete_logbook, incompete_timestamps =\
            gt.ops.partition_on_incomplete(logbook, timestamps)
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
    Partitioning a logbook on ``route_id``. Outputs a dict of logbooks keyed on route ID and a
    dict of timestamps keyed on route ID. Example usage:

    .. code:: python

        import gtfs_tripify as gt
        import requests

        response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
        response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
        response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
        stream = [response1.content, response2.content, response3.content]
        logbook, timestamps, parse_errors = gt.logify(stream)

        route_logbooks, route_timestamps = gt.ops.partition_on_route_id(logbook, timestamps)
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
    Given a list of trip logbooks and their corresponding timestamp data, perform a merge
    and return a combined logbook and the combined timestamps.

    The input logbooks must be in time-contiguous order. In other words, the first logbook
    should cover the time slice (t(1), ..., t(n)), the second the time slice
    (t(n + 1), ..., t(n + m)), and so on.

    Example usage:

    .. code:: python
    
        import gtfs_tripify as gt
        import requests

        response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
        response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
        response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
        response4 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')
        response5 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-51')
        response6 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')

        stream1 = [response1.content, response2.content, response3.content]
        stream2 = [response4.content, response5.content, response6.content]
        logbook1, timestamps1, parse_errors1 = gt.logify(stream1)
        logbook2, timestamps2, parse_errors2 = gt.logify(stream2)

        logbook, timestamps = gt.ops.merge([(logbook1, timestamps1), (logbook2, timestamps2)])
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
    first_right_timestamp = np.min(np.min([*(itertools.chain(right_timestamps.values()))]))

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
    # Naive cases.
    if len(left) == 0:
        return right
    elif len(right) == 0:
        return left

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
    # The next two lines handle cases (1) and (2), and the code block after that handles case (3).
    join.loc[:, 'minimum_time'] = join.loc[:, 'minimum_time'].fillna(method='ffill')
    join.loc[1:, 'minimum_time'] = np.maximum.accumulate(join.loc[1:, 'minimum_time'].values)

    # A sequence of stops at the end of the left stop sequence may not appear in the right stop
    # sequence. When the join is performed, `synthesize_route` will excise those stations b/c 
    # they constitute proven non-stops. While the prior two corrections are accumulators and thus
    # "safe" in this context, the following operation is a mutator and thus not safe. We must 
    # take care not to accidentally mutate the wrong entry or go out of bounds.
    # TODO: test this code path
    if len(join) > 1:
        left_isin_seq = left.stop_id.isin(right.stop_id)
        if len(left_isin_seq) > 0:
            update_idx = left_isin_seq[::-1].idxmax()
            if update_idx > 0:
                join.loc[update_idx, 'minimum_time'] = np.maximum(
                    np.nan_to_num(join.loc[update_idx - 1, 'maximum_time']),
                    join.loc[update_idx, 'minimum_time']
                )

    # Again at the location of the join, we may also get an incomplete `maximum_time` entry, 
    # for the same reason. In this case we will take the `maximum_time` of the following entry. 
    # However, note that we are *losing information* in this case, as we could technically 
    # resolve this time to a more accurate one, given the full list of information times. 
    # However, we do not have that information at this time in the processing sequence. This is
    # an unfortunate but not particularly important, all things considered, technical 
    # shortcoming.
    join.loc[:, 'maximum_time'] = join.loc[:, 'maximum_time'].fillna(method='bfill', limit=1)

    # TODO: is this typing operation necessary?
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
    Write a logbook to a GTFS ``stops.txt`` record. This method should only be run on complete
    logbooks (e.g., ones which you have already run ``gt.ops.cut_cancellations`` and
    ``gt.ops.discard_partial_logs`` on), as the GTFS spec does not allow null values or
    hypothetical stops in ``stops.txt``. For general use-cases, ``gt.ops.to_csv`` is preferable.

    Some edge case behaviors to keep in mind:

    * If there is no known minimum_time for a stop, a time 15 seconds before the maximum_time
      will be imputed. GTFS does not allow for null values.
    * If there is no known maximum time for a stop, the stop will not be included in the file.
    * If the train is still en route to a stop, that stop will not be included in the file.
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
    
    The output file is readable using an ordinary CSV reader, e.g. ``pandas.read_csv``.
    Alternatively you may read it back into a logbook format using ``gt.ops.from_csv``.
    """
    logs = []
    for unique_trip_id in logbook:
        log = logbook[unique_trip_id].assign(unique_trip_id=unique_trip_id)
        logs.append(log)

    if len(logs) == 0:
        df = pd.DataFrame(
            columns=[
                'trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                'latest_information_time', 'unique_trip'
            ]
        )
    else:
        df = pd.concat(logs)

    if output:
        return df
    else:
        return df.to_csv(filename, index=False)


def from_csv(filename):
    """
    Read a logbook from a CSV file (as written to by ``gt.ops.to_csv``).
    """
    g = pd.read_csv(filename).groupby('unique_trip_id')
    return {k: df.drop(columns='unique_trip_id') for k, df in g}


__all__ = [
    'cut_cancellations', 'discard_partial_logs', 'drop_invalid_messages', 
    'drop_duplicate_messages', 'drop_nonsequential_messages', 'partition_on_incomplete', 
    'merge_logbooks', 'parse_feed'
]
