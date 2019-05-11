"""
Module containing the main gtfs_tripify method, logify, which takes an update stream as input 
and returns a logbook as output. Processing is done in two steps. First, the update stream is
broken up by a unique_trip_id, which is inferred from the trip_id field and contextual information
about trips with the same route_id which are aligned with one another. This is complicated by the
fact that (1) trip_id values are not globally unique and are instead recycled by multiple trains
over the course of a day and (2) end-to-end runs may have their trip_id reassigned without 
warning. Then once the messages are aligned they are first transformed into action logs, and then
those action logs are compacted into logs in a logbook.
"""

import itertools
from collections import defaultdict
import uuid

import numpy as np
import pandas as pd

from gtfs_tripify.utils import synthesize_route, finish_trip
from gtfs_tripify.ops import (
    drop_invalid_messages, drop_duplicate_messages, drop_nonsequential_messages, parse_feed
)


########################
# INTERMEDIATE PARSERS #
########################
# dictify: GTFS-RT Protobuf -> <dict>
# actionify: (dict<trip_message>, dict<vehicle_message>, int<timestamp>) -> DataFrame<action_log>
# tripify: list<action_logs> -> DataFrame<trip_log>

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
        trip = finish_trip(trip, finish_information_time)

    return trip, timestamps


###############
# COLLOCATION #
###############
# These methods, which run early in the build process, break raw message streams into uniquified
# per-trip message lists.

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

    # Build a boolean matrix whose x_dim is trip_id and whose y_dim is time (update sequence 
    # number).
    containment_matrix = np.vstack(
        [np.isin(all_trip_ids, list(update_keymap.keys())) for update_keymap in update_keymaps]
    )

    # Parse the containment matrix to deduplicate trips with the same trip_id. E.g.:
    #   $TRIP_ID: [True, True, True, False] -> one trip
    #   $TRIP_ID: [True, True, False, True] -> two trips
    interim = defaultdict(list)

    for j in range(len(all_trip_ids)):
        previous_value = False
        current_unique_trip_id = str(uuid.uuid1())
        trip_id, trip_id_time_slice = all_trip_ids[j], containment_matrix[:, j]

        for sequence_number, entry in enumerate(trip_id_time_slice):
            if entry and previous_value is True:
                interim[current_unique_trip_id].append(update_keymaps[sequence_number][trip_id])
            elif entry and previous_value is False:
                previous_value = True
                current_unique_trip_id = str(uuid.uuid1())
                interim[current_unique_trip_id].append(update_keymaps[sequence_number][trip_id])
            elif not entry and previous_value is True:
                previous_value = False
            else:
                continue

    # combine trips, as indexed by trip_id, that that are "obviously" (hueristically) the same
    # trip:
    # * an old trip ended (disappeared) in update N
    # * a new trip started (appeared) in update N
    #   note: we intrinsically assume that the ID swap operation is atomic!
    # * the old and new trips share a route id (e.g. both are B trains)
    # * the new trip starts with the first remaining planned stop in the old trip
    st_map = dict()
    out = dict()

    # TODO: investigate what happens if there are >2 tripwise segments; probably need to loop
    # need to do this in two passes, first building a struct of all potential matches
    for uid in interim:
        route_id = interim[uid][0]['trip_update']['trip_update']['trip']['route_id']
        start_timestamp = interim[uid][0]['timestamp']

        if route_id in st_map:
            if start_timestamp in st_map[route_id]:
                st_map[route_id][start_timestamp].append(uid)
            else:
                st_map[route_id][start_timestamp] = [uid]
        else:
            st_map[route_id] = {start_timestamp: [uid]}

    # then analyzing those potential matches one-by-one in detail
    timestamp_sequence = [u['header']['timestamp'] for u in updates]
    already_merged = set()
    for uid in interim:
        if uid in already_merged:
            continue  # the trip has already been matched so we are done (but see to-do)

        route_id = interim[uid][0]['trip_update']['trip_update']['trip']['route_id']
        last_timestamp = interim[uid][-1]['timestamp']

        end_index = timestamp_sequence.index(last_timestamp) + 1
        if end_index >= len(timestamp_sequence):
            continue  # the trip never terminated so we are done

        end_timestamp = timestamp_sequence[
            timestamp_sequence.index(last_timestamp) + 1
        ]
        if end_timestamp not in st_map[route_id]:
            continue  # no other trips on this route started at this time so we are done

        possible_matches = set(st_map[route_id][end_timestamp]).difference(already_merged)
        for candidate_uid in possible_matches:
            current_first_remaining_stop = interim[uid][-1]['trip_update']['trip_update']\
                ['stop_time_update'][0]['stop_id']
            candidate_initial_stop = interim[candidate_uid][0]['trip_update']\
                ['trip_update']['stop_time_update'][0]['stop_id']

            if (candidate_uid != uid and
                candidate_initial_stop == current_first_remaining_stop):
                # the trips match; stitch them together
                out[uid] = interim[uid] + interim[candidate_uid]
                already_merged.update({candidate_uid})
                break

    for uid in interim:
        if uid not in out and uid not in already_merged:
            out[uid] = interim[uid]

    return out


######################
# USER_FACING METHOD #
######################

def logify(updates):
    """
    Given a list of feed updates, returns a logbook associated with each trip mentioned in those
    feeds. Also returns the set of timestamps covered by the logbook. Output is the tuple 
    (logbook, timestamps).
    """
    # trivial case
    if updates == []:
        return dict(), dict(), None

    def _parse_message_list_into_action_log(message_collection, timestamps):
        actions_list = []
        for message, timestamp in zip(message_collection, timestamps):
            trip_update = message['trip_update']
            vehicle_update = message['vehicle_update']
            actions = actionify(trip_update, vehicle_update, timestamp)
            actions_list.append(actions)
        return pd.concat(actions_list)

    # Accept either raw Protobuf updates or already-parsed dict updates.
    already_parsed = isinstance(updates[0], dict)
    parse_errors = None if already_parsed else []
    if not already_parsed:
        # step 1: bytes -> protobufs
        protobufs = []
        for update in updates:
            try:
                protobuf = parse_feed(update)
                if protobuf is None:  # an unsafe Protobuf parse
                    parse_errors.append({
                        'type': 'parsing_into_protobuf_raised_runtime_warning'
                    })
                else:
                    protobufs.append(protobuf)
            except (SystemExit, KeyboardInterrupt) as e:
                raise e
            except:  # an erroneous Protobuf parse
                parse_errors.append({
                    'type': 'parsing_into_protobuf_raised_exception'
                })
        del updates

        # step 2: protobufs -> dicts
        # Since the Protobuf parser should raise for major GTFS-RT schema violations, this method
        # can only raise in the case of a logic fault in the code, not in the data, so we do not
        # intercept errors here.
        update_dicts = [dictify(protobuf) for protobuf in protobufs]
        del protobufs

        # step 3: dicts -> cleaned-up dicts
        clean_updates = []
        for update in update_dicts:
            update, drop_invalid_parse_errors = drop_invalid_messages(update)
            parse_errors += drop_invalid_parse_errors
            clean_updates.append(update)
        del update_dicts

        # step 4: dict feed -> deduplicated dict feed with known good timestamps
        clean_deduped_updates, drop_duplicate_messages_parse_errors =\
            drop_duplicate_messages(clean_updates)

        parse_errors += drop_duplicate_messages_parse_errors
        del clean_updates

        clean_updates, drop_nonsequential_messages_parse_errors =\
            drop_nonsequential_messages(clean_deduped_updates)

        parse_errors += drop_nonsequential_messages_parse_errors
        del clean_deduped_updates

        updates = clean_updates

    last_timestamp = updates[-1]['header']['timestamp']

    # collate generates `unique_trip_id` values for each trip and keys them to message collections
    # TODO: it is probably necessary to cut cancellations before collation?
    message_collections = collate(updates)

    logbook = dict()
    timestamps = dict()

    for unique_trip_id in message_collections:
        message_collection = message_collections[unique_trip_id]
        message_timestamps = [message['timestamp'] for message in message_collection]

        actions_logs = []
        last_tripwise_timestamp = message_collection[-1]['timestamp']
        trip_terminated = message_collection[-1]['timestamp'] < last_timestamp

        action_log = _parse_message_list_into_action_log(
            message_collection, message_timestamps
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
            trip_log = finish_trip(trip_log, trip_terminated_time)

        logbook[unique_trip_id] = trip_log
        timestamps[unique_trip_id] = trip_timestamps

    return logbook, timestamps, parse_errors
