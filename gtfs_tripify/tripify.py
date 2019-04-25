import numpy as np
import itertools
from collections import defaultdict
import pandas as pd
from gtfs_tripify.utils import synthesize_route
import warnings
import uuid


def dictify(buffer):
    """
    Parses a GTFS-Realtime Protobuf into a Python dict, which is more ergonomic to work with.
    Fields not in the GTFS-RT schema are ignored.
    """
    update = {
        'header': {'gtfs_realtime_version': buffer.header.gtfs_realtime_version,
                   'timestamp': buffer.header.timestamp},
        'entity': []
    }

    # Helper functions for determining GTFS-RT message types.
    def is_vehicle_update(message):
        return str(message.trip_update.trip.route_id) == '' and str(message.alert) == ''

    def is_alert(message):
        return str(message.alert) != ''

    def is_trip_update(message):
        return not is_vehicle_update(message) and not is_alert(message)

    # Helper function for mapping dictionary-encoded statuses into human-readable strings.
    def munge_status(status_code):
        statuses = {
            0: 'INCOMING_AT',
            1: 'STOPPED_AT',
            2: 'IN_TRANSIT_TO'
        }
        return statuses[status_code]

    for message in buffer.entity:
        if is_trip_update(message):
            parsed_message = {
                'id': message.id,
                'trip_update': {
                    'trip': {
                        'trip_id': message.trip_update.trip.trip_id,
                        'start_date': message.trip_update.trip.start_date,
                        'route_id': message.trip_update.trip.route_id
                    },
                    'stop_time_update': [
                        {
                            'stop_id': _update.stop_id,
                            'arrival': np.nan if str(_update.arrival) == "" 
                                else _update.arrival.time,
                            'departure': np.nan if str(_update.departure) == "" 
                                else _update.departure.time
                        } for _update in message.trip_update.stop_time_update]
                },
                'type': 'trip_update'
            }
            update['entity'].append(parsed_message)
        elif is_vehicle_update(message):
            parsed_message = {
                'id': message.id,
                'vehicle': {
                    'trip': {
                        'trip_id': message.vehicle.trip.trip_id,
                        'start_date': message.vehicle.trip.start_date,
                        'route_id': message.vehicle.trip.route_id
                    },
                    'current_stop_sequence': message.vehicle.current_stop_sequence,
                    'current_status': munge_status(message.vehicle.current_status),
                    'timestamp': message.vehicle.timestamp,
                    'stop_id': message.vehicle.stop_id
                },
                'type': 'vehicle_update'
            }
            update['entity'].append(parsed_message)
        else:  # is_alert
            parsed_message = {
                'id': message.id,
                'alert': {
                    'header_text': {
                        'translation': {
                            'text': message.alert.header_text.translation[0].text
                        }
                    },
                    'informed_entity': [
                        {
                            'trip_id': _trip.trip.trip_id,
                            'route_id': _trip.trip.route_id
                        } for _trip in message.alert.informed_entity]
                },
                'type': 'alert'
            }
            update['entity'].append(parsed_message)

    return update


def drop_invalid_messages(update):
    """
    Given a feed update (as returned by `dictify`), catch certain non-fatal errors that, though 
    they violate the GTFS-RT spec, are still valid in the Protobuf spec. These are operator errors
    made by the feed publisher. A warning is raised and the non-conformant messages are dropped.
    """
    # Capture and throw away vehicle updates that do not also have trip updates.
    vehicle_update_ids = {
        m['vehicle']['trip']['trip_id'] for m in update['entity'] if m['type'] == 'vehicle_update'
    }
    trip_update_ids = {
        m['trip_update']['trip']['trip_id'] for m in update['entity'] if m['type'] == 'trip_update'
    }
    trip_update_only_ids = vehicle_update_ids.difference(trip_update_ids)

    if len(trip_update_only_ids) > 0:
        warnings.warn(
            f"The trips with IDs {trip_update_only_ids} are provided vehicle updates but not "
            f"trip updates in the GTFS-R update for {update['header']['timestamp']}. "
            f"These invalid trips were removed from the update during pre-processing."
        )
        update['entity'] = [m for m in update['entity'] if (
                m['type'] != 'vehicle_update' or 
                m['vehicle']['trip']['trip_id'] not in trip_update_only_ids
            )
        ]

    # Capture and throw away messages which have a null (empty string, '') trip id.
    nonalert_ids = vehicle_update_ids | trip_update_ids
    if '' in nonalert_ids:
        warnings.warn(
            f"Some of the messages in the GTFS-R update for {update['header']['timestamp']} "
            f"have a null trip id. These invalid messages were removed from the update during "
            f"pre-processing."
        )
        update['entity'] = [m for m in update['entity'] if (
            (m['type'] == 'vehicle_update' and m['vehicle']['trip']['trip_id'] != "") or
            (m['type'] == 'trip_update') and m['trip_update']['trip']['trip_id'] != "")
        ]

    return update


def collate_update(update, include_alerts=False):
    """
    Collates the messages in an update into a list with the following shape, which is convenient for
    further processing:

        [
            {'trip_id': $TRIP_ID, 
             'trip_update': $TRIP_UPDATE_MESSAGE,
             'vehicle_update': $VEHICLE_UPDATE_MESSAGE,
             'timestamp': $TIMESTAMP}, 
            ...
        ]

    Implementation detail of `collate`.
    """
    if include_alerts:
        raise NotImplementedError

    # initially build a dict keyed in trip_id
    keymap = defaultdict(dict)

    for message in update['entity']:
        if message['type'] == 'alert':
            continue
        if message['type'] == 'trip_update':
            trip_id = message['trip_update']['trip']['trip_id']
            keymap[trip_id]['trip_update'] = message
        elif message['type'] == 'vehicle_update':
            keymap[trip_id]['vehicle_update'] = message

    ts = update['header']['timestamp']
    keymap = {key: {'vehicle_update': None, 'timestamp': ts, **keymap[key]} for key in keymap}
    return keymap


def collate(updates, include_alerts=False):
    """
    Sorts the messages in a list of updates into a nested dict of messages keyed on a unique ID. 
    Output is in the following format:

    {
        '$UNIQUE_TRIP_ID': [
            {'trip_id': $TRIP_ID,
             'trip_update': $TRIP_UPDATE_MESSAGE,
             'vehicle_update': $VEHICLE_UPDATE_MESSAGE,
             'timestamp': $TIMESTAMP},
            ...
        ],
        ...
    }

    Note that the interior message is in the format returned by the `collate_update` subroutine, 
    which handles collocation *within* an update, whilst this method handles collocation *between*
    updates.

    This method calculates a UUID for the `unique_trip_id`.
    """
    if include_alerts:
        raise NotImplementedError("Processing alert messages has not been implemented yet.")

    if len(updates) == 0:
        return []

    update_keymaps = [collate_update(update, include_alerts=include_alerts) for update in updates]
    all_trip_ids = set()
    for update_keymap in update_keymaps:
        all_trip_ids.update(set(update_keymap.keys()))
    all_trip_ids = list(all_trip_ids)

    # Build a boolean matrix whose x_dim is trip_id and whose y_dim is time (update sequence number).
    containment_matrix = np.vstack(
        [np.isin(all_trip_ids, list(update_keymap.keys())) for update_keymap in update_keymaps]
    )

    # Parse the containment matrix to deduplicate trips with the same trip_id. E.g.:
    #   $TRIP_ID: [True, True, True, False] -> one trip
    #   $TRIP_ID: [True, True, False, True] -> two trips
    out = defaultdict(list)

    for j in range(len(all_trip_ids)):
        previous_value = False
        current_unique_trip_id = str(uuid.uuid1())
        trip_id, trip_id_time_slice = all_trip_ids[j], containment_matrix[:, j]

        for sequence_number, entry in enumerate(trip_id_time_slice):
            if entry and previous_value is True:
                out[current_unique_trip_id].append(update_keymaps[sequence_number][trip_id])
            elif entry and previous_value is False:
                previous_value = True
                current_unique_trip_id = str(uuid.uuid1())
                out[current_unique_trip_id].append(update_keymaps[sequence_number][trip_id])
            elif not entry and previous_value is True:
                previous_value = False
            else:
                continue

    return out


def actionify(trip_message, vehicle_message, timestamp):
    """
    Parses the trip update and vehicle update messages (if there is one; may be None) for a 
    particular trip into an action log.
    """
    # If a vehicle message is not None, the trip is already in progress.
    inp = vehicle_message is not None

    # The base of the log entry is the same for all possible entries.
    base = np.array([trip_message['trip_update']['trip']['trip_id'],
                     trip_message['trip_update']['trip']['route_id'], timestamp])
    vehicle_status = vehicle_message['vehicle']['current_status'] if inp else 'QUEUED'
    loglist = []

    def log_arrival(stop_id, arrival_time):
        loglist.append(
            np.append(base.copy(), np.array(['EXPECTED_TO_ARRIVE_AT', stop_id, arrival_time]))
        )

    def log_departure(stop_id, departure_time):
        loglist.append(
            np.append(base.copy(), np.array(['EXPECTED_TO_DEPART_AT', stop_id, departure_time]))
        )

    def log_stop(stop_id, arrival_time):
        loglist.append(
            np.append(base.copy(), np.array(['STOPPED_AT', stop_id, arrival_time]))
        )

    def log_skip(stop_id, skip_time):
        loglist.append(
            np.append(base.copy(), np.array(['EXPECTED_TO_SKIP', stop_id, skip_time]))
        )

    for s_i, stop_time_update in enumerate(trip_message['trip_update']['stop_time_update']):

        first_station = s_i == 0
        last_station = s_i == len(trip_message['trip_update']['stop_time_update']) - 1
        stop_id = stop_time_update['stop_id']
        arrival_time = stop_time_update['arrival']
        departure_time = stop_time_update['departure']

        # First station, vehicle status is STOPPED_AT.
        if first_station and vehicle_status == 'STOPPED_AT':
            log_stop(stop_id, arrival_time)

        # First station, vehicle status is QUEUED.
        elif first_station and vehicle_status == 'QUEUED':
            log_departure(stop_id, departure_time)

        # First station, vehicle status is IN_TRANSIT_TO or INCOMING_AT, both arrival and 
        # departure fields are non-null.
        # Intermediate station, both arrival and departure fields are non-null.
        elif ((first_station and
               vehicle_status in ['IN_TRANSIT_TO', 'INCOMING_AT'] and
               pd.notnull(arrival_time) and pd.notnull(departure_time)) or

              (not first_station and
               not last_station and
               pd.notnull(arrival_time) and pd.notnull(departure_time))):

            log_arrival(stop_id, arrival_time)
            log_departure(stop_id, departure_time)

        # Not the last station, one of arrival or departure is null.
        elif ((not last_station and
               (pd.isnull(arrival_time) or pd.isnull(departure_time)))):
            log_skip(stop_id, departure_time) if pd.isnull(arrival_time)\
                else log_skip(stop_id, arrival_time)

        # Last station, not also the first (e.g. not length 1).
        elif last_station and not first_station:
            log_arrival(stop_id, arrival_time)

        # Last station, also first station, vehicle status is IN_TRANSIT_TO or INCOMING_AT.
        elif last_station and vehicle_status in ['IN_TRANSIT_TO', 'INCOMING_AT']:
            log_arrival(stop_id, arrival_time)

        # This shouldn't occur, and indicates an error in the input or our logic.
        else:
            raise ValueError(
                "An error occurred while converting a message to an action log, probably due "
                "to invalid input."
            )

    action_log = pd.DataFrame(
        loglist, 
        columns=['trip_id', 'route_id', 'information_time', 'action', 'stop_id','time_assigned']
    )
    # base is a single-typed numpy array which converts the information_time input to dtype `U<14`
    # so we have to convert it back before returning
    action_log = action_log.assign(information_time=action_log.information_time.astype(int))
    return action_log


def _parse_message_list_into_action_log(message_collection, timestamp):
    """
    Parses a list of messages into a single pandas.DataFrame.
    """

    actions_list = []

    for message in message_collection:
        trip_update = message['trip_update']
        vehicle_update = message['vehicle_update']

        actions = actionify(trip_update, vehicle_update, timestamp)
        actions_list.append(actions)
    
    return pd.concat(actions_list)


def tripify(tripwise_action_logs, finished=False, finish_information_time=None):
    """
    Given a list of action logs associated with a particular trip, returns the result of their 
    merger: a single trip log.

    By default, this trip is left unterminated. To terminate the trip (replacing any remaining 
    stops to be made with the appropriate information), set the `finished` flag to `True` and 
    provide a `finish_information_time`, which should correspond with the time at which you learn 
    that the trip has ended. This must be provided separately because when a trip ends, it merely 
    disappears from the GTFS-R feed, The information time of the first GTFS-R feed *not* 
    containing this trip, an externality, is the relevant piece of information.
    """

    # Capture the first row of information for each information time. `key_data` may contain 
    # skipped stops! We have to iterate through `remaining_stops` and `key_data` simultaneously 
    # to get what we want.
    all_data = pd.concat(tripwise_action_logs)
    key_data = (all_data
                .groupby('information_time')
                .first()
                .reset_index())
    timestamps = key_data.information_time.values.tolist()

    # Get the complete (synthetic) stop list.
    stops = synthesize_route([list(log['stop_id'].unique()) for log in tripwise_action_logs])

    # Get the complete list of information times.
    information_times = [np.nan] + list(all_data['information_time'].unique()) + [np.nan]

    # Init lines, where we will concat our final result, and the base (trip_id, route_id) to be 
    # written to it.
    base = np.array([key_data.iloc[0]['trip_id'], key_data.iloc[0]['route_id']])
    lines = []

    # Key data index pointers.
    kd_i = 0  # key data index
    it_i = 1  # information time index
    st_i = 0  # synthetic stop list index

    # Book-keep stops that we have already accounted for.
    passed_stops = set()
    most_recent_passed_stop = None

    while kd_i < len(key_data) and st_i < len(stops):
        next_stop = stops[st_i]
        next_record = key_data.iloc[kd_i]

        if next_record['stop_id'] != next_stop and next_record['stop_id'] not in passed_stops:
            skipped_stop = np.append(base.copy(), np.array(
                ['STOPPED_OR_SKIPPED', information_times[it_i - 1], 
                 information_times[it_i], next_stop, information_times[it_i]]
            ))
            lines.append(skipped_stop)
            passed_stops.add(next_stop)
            most_recent_passed_stop = next_stop

            st_i += 1

        elif next_record['stop_id'] != next_stop and next_record['stop_id'] == most_recent_passed_stop:
            lines[-1][4] = information_times[it_i + 1]
            it_i += 1
            kd_i += 1

        elif next_record['stop_id'] == next_stop and next_record['action'] == 'STOPPED_AT':
            stopped_stop = np.append(base.copy(), np.array(
                ['STOPPED_AT', information_times[it_i - 1], information_times[it_i + 1],
                 next_stop, information_times[it_i]]
            ))
            lines.append(stopped_stop)
            passed_stops.add(next_stop)
            most_recent_passed_stop = next_stop

            it_i += 1
            kd_i += 1
            st_i += 1

        # next_record['stop_id'] == next_stop and next_record['action'] == 'EXPECTED_TO_ARRIVE_AT':
        else:
            it_i += 1
            kd_i += 1

    # Any stops left over we haven't arrived at yet.
    latest_information_time = int(information_times[-2])

    for remaining_stop in [stop for stop in stops if stop not in passed_stops]:
        future_stop = np.append(base.copy(), np.array(
            ['EN_ROUTE_TO', latest_information_time, np.nan,
             remaining_stop, latest_information_time]
        ))
        lines.append(future_stop)

    trip = pd.DataFrame(lines, columns=[
        'trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id', 
        'latest_information_time'
    ])

    if finished:
        assert finish_information_time
        trip = _finish_trip(trip, finish_information_time)

    return trip, timestamps


def _finish_trip(trip_log, timestamp):
    """
    Finishes a trip. We know a trip is finished when its messages stops appearing in feed files,
    at which time we can "cross out" any stations still remaining.
    """
    trip_log = (trip_log.replace('EN_ROUTE_TO', 'STOPPED_OR_SKIPPED')
                        .replace('EXPECTED_TO_SKIP', 'STOPPED_OR_SKIPPED')
                        .replace('nan', np.nan))
    trip_log['maximum_time'] = trip_log['maximum_time'].fillna(timestamp)
    return trip_log


def logify(updates):
    """
    Given a list of feed updates, returns a logbook associated with each trip mentioned in those
    feeds. Also returns the set of timestamps covered by the logbook. Output is the tuple 
    (logbook, timestamps).
    """
    # trivial case
    if updates == []:
        return dict()

    # Accept either raw Protobuf updates or already-parsed dict updates.
    if not isinstance(updates[0], dict):
        updates = [dictify(update) for update in updates]

    last_timestamp = updates[-1]['header']['timestamp']

    # collate generates `unique_trip_id` values for each trip and keys them to message collections
    message_collections = collate(updates)

    logbook = dict()
    timestamps = dict()

    for unique_trip_id in message_collections:
        message_collection = message_collections[unique_trip_id]
        actions_logs = []
        last_tripwise_timestamp = message_collection[-1]['timestamp']
        trip_terminated = message_collection[-1]['timestamp'] < last_timestamp

        action_log = _parse_message_list_into_action_log(
            message_collection, last_tripwise_timestamp
        )
        actions_logs.append(action_log)
        trip_log, trip_timestamps = tripify(actions_logs)
        # TODO: is this necessary? Coerce types.
        trip_log = trip_log.assign(
            minimum_time=trip_log.minimum_time.astype('float'),
            maximum_time=trip_log.maximum_time.astype('float'),
            latest_information_time=trip_log.latest_information_time.astype('int')
        )

        # If the trip was terminated sometime in the course of these feeds, update the trip log
        if trip_terminated:
            trip_terminated_time = last_tripwise_timestamp
            trip_log = _finish_trip(trip_log, trip_terminated_time)

        logbook[unique_trip_id] = trip_log
        timestamps[unique_trip_id] = trip_timestamps

    return logbook, timestamps


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
    # TODO: this will not address case (3), in the potential case that the empty is correct.
    # This is a bug that needs to be fixed!
    if len(right) == 0:
        return left
    if len(left) == 0:
        return right

    # There are five kinds of joins that we care about.
    # (1) complete trips on the left side, just append
    # (2) complete trips on the right side, just append
    # (3) incomplete trips on the left side that do not appear on the right, these are cancels
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
    # if no such trip exists, this is a cancellation, so perform the requisite op

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
            left[unique_trip_id_left] = _finish_trip(left[unique_trip_id_left], first_right_timestamp)


    # for trips we did not find a a mat
    # finalize trips that were incomplete in the left and also didn't appear in the right
    # this is whatever's left that's in the left_map after joins are done
    for trip_id in left_map:
        unique_trip_id = left_map[trip_id]
        left[unique_trip_id] = _finish_trip(left[unique_trip_id], first_right_timestamp)

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
    l_i = r_i = 0
    left_indices, right_indices = [], []

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
