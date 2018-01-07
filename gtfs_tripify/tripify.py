import numpy as np
import itertools
from collections import defaultdict
import pandas as pd
from gtfs_tripify.utils import synthesize_route
import warnings


def dictify(feed):
    """
    Parses a GTFS-Realtime feed that has been loaded into a `gtfs_realtime_pb2` object into a native dictionary
    representation.
    """
    _feed = feed
    feed = {
        'header': {'gtfs_realtime_version': _feed.header.gtfs_realtime_version,
                   'timestamp': _feed.header.timestamp},
        'entity': []
    }

    # Helper functions for determining message types in the gtfs_realtime_pb2` object.
    def is_vehicle_update(message):
        return str(message.trip_update.trip.route_id) == '' and str(message.alert) == ''

    def is_alert(message):
        return str(message.alert) != ''

    def is_trip_update(message):
        return not is_vehicle_update(message) and not is_alert(message)

    # Helper function for assigning status.
    def munge_status(status_code):
        statuses = {
            0: 'INCOMING_AT',
            1: 'STOPPED_AT',
            2: 'IN_TRANSIT_TO'
        }
        return statuses[status_code]

    for _message in _feed.entity:
        if is_trip_update(_message):
            message = {
                'id': _message.id,
                'trip_update': {
                    'trip': {
                        'trip_id': _message.trip_update.trip.trip_id,
                        'start_date': _message.trip_update.trip.start_date,
                        'route_id': _message.trip_update.trip.route_id
                    },
                    'stop_time_update': [
                        {
                            'stop_id': _update.stop_id,
                            'arrival': np.nan if str(_update.arrival) == "" else _update.arrival.time,
                            'departure': np.nan if str(_update.departure) == "" else _update.departure.time
                        } for _update in _message.trip_update.stop_time_update]
                },
                'type': 'trip_update'
            }
            feed['entity'].append(message)
        elif is_vehicle_update(_message):
            message = {
                'id': _message.id,
                'vehicle': {
                    'trip': {
                        'trip_id': _message.vehicle.trip.trip_id,
                        'start_date': _message.vehicle.trip.start_date,
                        'route_id': _message.vehicle.trip.route_id
                    },
                    'current_stop_sequence': _message.vehicle.current_stop_sequence,
                    'current_status': munge_status(_message.vehicle.current_status),
                    'timestamp': _message.vehicle.timestamp,
                    'stop_id': _message.vehicle.stop_id
                },
                'type': 'vehicle_update'
            }
            feed['entity'].append(message)
        else:  # is_alert
            message = {
                'id': _message.id,
                'alert': {
                    'header_text': {
                        'translation': {
                            # TODO
                            'text': _message.alert.header_text.translation[0].text
                        }
                    },
                    'informed_entity': [
                        {
                            'trip_id': _trip.trip.trip_id,
                            'route_id': _trip.trip.route_id
                        } for _trip in _message.alert.informed_entity]
                },
                'type': 'alert'
            }
            feed['entity'].append(message)

    # Correct and warn about feed errors.
    feed = correct(feed)

    return feed


def correct(feed):
    """
    Verifies that the inputted dictified feed has the expected schema. Raises warnings wherever issues are found,
    and attempts to cure them.
    """
    # Capture and throw away vehicle updates that do not also have trip updates.
    vehicle_update_ids = {m['vehicle']['trip']['trip_id'] for m in feed['entity'] if m['type'] == 'vehicle_update'}
    trip_update_ids = {m['trip_update']['trip']['trip_id'] for m in feed['entity'] if m['type'] == 'trip_update'}
    trip_update_only_ids = vehicle_update_ids.difference(trip_update_ids)

    if len(trip_update_only_ids) > 0:
        warnings.warn("The trips with IDs {0} are provided vehicle updates but not trip updates in the GTFS-R feed "
                      "for {1}. These invalid trips were removed from the feed during pre-processing.".format(
            trip_update_only_ids, feed['header']['timestamp'])
        )
        feed['entity'] = [m for m in feed['entity'] if (m['type'] != 'vehicle_update' or
                                                        m['vehicle']['trip']['trip_id'] not in trip_update_only_ids)]

    # Capture and throw away messages which have a null (empty string, '') trip id.
    nonalert_ids = vehicle_update_ids | trip_update_ids
    if '' in nonalert_ids:
        warnings.warn("Some of the messages in the GTFS-R feed for {0} have a null trip id. These invalid messages "
                      "were removed from the feed during pre-processing.".format(
            trip_update_only_ids, feed['header']['timestamp'])
        )
        feed['entity'] = [m for m in feed['entity'] if ((m['type'] == 'vehicle_update' and
                                                         m['vehicle']['trip']['trip_id'] != "") or
                                                        (m['type'] == 'trip_update') and
                                                         m['trip_update']['trip']['trip_id'] != "")]

    return feed


def _tripsort(feed, include_alerts=False):
    """
    Sorts the messages a set of dictified feeds into a hash table. Does not handle collisions!
    """
    messages = feed['entity']
    sort = defaultdict(list)

    def get_trip_ids(message):
        if message['type'] == 'trip_update':
            return [message['trip_update']['trip']['trip_id']]
        elif message['type'] == 'vehicle_update':
            return [message['vehicle']['trip']['trip_id']]
        else:  # alert
            return [entity['trip_id'] for entity in message['alert']['informed_entity']]

    messages = messages if include_alerts else [m for m in messages if m['type'] != 'alert']

    for message in messages:
        for trip_id in get_trip_ids(message):
            sort[trip_id].append(message)

    return sort


def _feedsort(feeds, include_alerts=False):
    """
    Sorts the messages in a timely list of dictified feeds into a list of trip-id-to-message hash tables. This
    method handles the Trip ID collisions that occur when a trip ID is recycled within the time span of the feed.
    """
    if include_alerts:
        raise NotImplementedError("Processing alert messages has not been implemented yet.")

    if len(feeds) == 0:
        return []

    message_tables = [_tripsort(feed, include_alerts=False) for feed in feeds]
    trip_ids = list(set(itertools.chain(*[table.keys() for table in message_tables])))

    # x dimension is categorical trip_id, y dimension is time (feed sequence number).
    containment_matrix = np.concatenate([[np.in1d(trip_ids, list(table.keys()))] for table in message_tables], axis=0)

    for i in range(len(trip_ids)):
        n = 0
        trip_id, strip = trip_ids[i], containment_matrix[:, i]
        new_ids = []

        for contains in strip:
            if contains:
                new_ids.append("{0}_{1}".format(trip_id, n))
            else:
                new_ids.append(None)
                n += 1

        for i, table in enumerate(message_tables):
            if new_ids[i] is not None:
                table[new_ids[i]] = table.pop(trip_id)
            else:
                continue

    return message_tables


def actionify(trip_message, vehicle_message, timestamp):
    """
    Parses the trip update and vehicle update messages (if there is one; may be None) for a particular trip into an
    action log.

    This method is called by parse_message_list_into_action_log in a loop in order to get the complete action log.
    """
    # If a vehicle message is not None, the trip is already in progress.
    inp = bool(vehicle_message)

    # The base of the log entry is the same for all possible entries.
    base = np.array([trip_message['trip_update']['trip']['trip_id'],
                     trip_message['trip_update']['trip']['route_id'], timestamp])
    vehicle_status = vehicle_message['vehicle']['current_status'] if inp else 'QUEUED'
    loglist = []

    def log_arrival(stop_id, arrival_time):
        loglist.append(np.append(base.copy(), np.array(['EXPECTED_TO_ARRIVE_AT', stop_id, arrival_time])))

    def log_departure(stop_id, departure_time):
        loglist.append(np.append(base.copy(), np.array(['EXPECTED_TO_DEPART_AT', stop_id, departure_time])))

    def log_stop(stop_id, arrival_time):
        loglist.append(np.append(base.copy(), np.array(['STOPPED_AT', stop_id, arrival_time])))

    def log_skip(stop_id, skip_time):
        loglist.append(np.append(base.copy(), np.array(['EXPECTED_TO_SKIP', stop_id, skip_time])))

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

        # First station, vehicle status is IN_TRANSIT_TO or INCOMING_AT, both arrival and departure fields are notnull.
        # Intermediate station, both arrival and departure fields are notnull.
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
            log_skip(stop_id, departure_time) if pd.isnull(arrival_time) else log_skip(stop_id, arrival_time)

        # Last station, not also the first (e.g. not length 1).
        elif last_station and not first_station:
            log_arrival(stop_id, arrival_time)

        # Last station, also first station, vehicle status is IN_TRANSIT_TO or INCOMING_AT.
        elif last_station and vehicle_status in ['IN_TRANSIT_TO', 'INCOMING_AT']:
            log_arrival(stop_id, arrival_time)

        # This shouldn't occur, and indicates an error in the input or our logic.
        else:
            raise ValueError("An error occurred while converting a message to an action log, probably due to invalid "
                             "input.")

    action_log = pd.DataFrame(loglist, columns=['trip_id', 'route_id', 'information_time', 'action', 'stop_id',
                                              'time_assigned'])
    return action_log


def _parse_message_list_into_action_log(messages, timestamp):
    """
    Parses a list of messages into a single pandas.DataFrame. Internal routine.
    """

    actions_list = []
    nonalerts = [message for message in messages if message['type'] != 'alert']

    for i in range(0, len(nonalerts)):
        trip_update = messages[i]

        # Selectively loop through trip updates.
        if trip_update['type'] == 'vehicle_update':
            pass

        # If the entry is a trip update, find the associated vehicle update, if it exists, and pass that to the list.
        else:
            has_vehicle_update = False if (i == len(messages) - 1) else (messages[i + 1]['type'] == 'vehicle_update')
            vehicle_update = messages[i + 1] if has_vehicle_update else None

            actions = actionify(trip_update, vehicle_update, timestamp)
            actions_list.append(actions)

    return pd.concat(actions_list)


def tripify(tripwise_action_logs, finished=False, finish_information_time=None):
    """
    Given a list of action logs associated with a particular trip, returns the result of their merger: a single trip
    log.

    By default, this trip is left unterminated. To terminate the trip (replacing any remaining stops to be made with
    the appropriate information), set the `finished` flag to `True` and provide a `finish_information_time`,
    which should correspond with the time at which you learn that the trip has ended. This must be provided
    separately because when a trip ends, it merely disappears from the GTFS-R feed, The information time of the
    first GTFS-R feed *not* containing this trip, an externality, is the relevant piece of information.
    """

    # Capture the first row of information for each information time. `key_data` may contain skipped stops! We have
    # to iterate through `remaining_stops` and `key_data` simultaneously to get what we want.
    all_data = pd.concat(tripwise_action_logs)
    key_data = (all_data
                .groupby('information_time')
                .first()
                .reset_index())

    # Get the complete (synthetic) stop list.
    stops = synthesize_route([list(log['stop_id'].unique()) for log in tripwise_action_logs])

    # Get the complete list of information times.
    information_times = [np.nan] + list(all_data['information_time'].unique()) + [np.nan]

    # Init lines, where we will concat our final result, and the base (trip_id, route_id) to be written to it.
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
        # import pdb; pdb.set_trace()
        next_stop = stops[st_i]
        next_record = key_data.iloc[kd_i]

        if next_record['stop_id'] != next_stop and next_record['stop_id'] not in passed_stops:
            skipped_stop = np.append(base.copy(), np.array(
                            ['STOPPED_OR_SKIPPED', information_times[it_i - 1], information_times[it_i],
                             next_stop, information_times[it_i]]
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

        else:  # next_record['stop_id'] == next_stop and next_record['action'] == 'EXPECTED_TO_ARRIVE_AT':
            it_i += 1
            kd_i += 1

    # Any stops left over we haven't arrived at yet.
    latest_information_time = information_times[-2]

    for remaining_stop in [stop for stop in stops if stop not in passed_stops]:
        future_stop = np.append(base.copy(), np.array(
            ['EN_ROUTE_TO', latest_information_time, np.nan,
             remaining_stop, latest_information_time]
        ))
        lines.append(future_stop)

    trip = pd.DataFrame(lines, columns=['trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                                        'latest_information_time'])

    if finished:
        assert finish_information_time
        trip = _finish_trip(trip, finish_information_time)

    return trip


def _finish_trip(trip_log, timestamp):
    """
    Finishes a trip. We know a trip is finished when its messages stops appearing in feed files, at which time we can
    "cross out" any stations still remaining.
    """
    trip_log = (trip_log.replace('EN_ROUTE_TO', 'STOPPED_OR_SKIPPED')
                        .replace('EXPECTED_TO_SKIP', 'STOPPED_OR_SKIPPED')
                        .replace('nan', np.nan))
    trip_log['maximum_time'] = trip_log['maximum_time'].fillna(timestamp)
    return trip_log


def logify(feeds):
    """
    Given a list of feeds, returns a hash table of trip logs associated with each trip mentioned in those feeds.
    """
    timestamps = [feed['header']['timestamp'] for feed in feeds]

    # The trip IDs that are assigned by the MTA are unique during their lifetime, but get recycled over the course of
    # the day. So for example if a trip is assigned the trip ID `000000_L..S`, and that trip ends, that trip ID is
    # immediately available for reassignment to the next L train to be added to the schedule. Indeed, it may be the
    # first ID *in line* for reassignment!
    #
    # We have to perform our own heuristic to bifurcate non-contiguous trips. The marker that we will use is the trip
    # messages appearing non-contiguously. This is *not* a complete solution, as it is technically possible for a
    # trip id to be released and reused inside of the "update window". However, it's difficult to do better. We will
    # see whether or not this works well enough though.
    message_tables = _feedsort(feeds)
    trip_ids = set(itertools.chain(*[table.keys() for table in message_tables]))

    ret = dict()

    for trip_id in trip_ids:
        actions_logs = []
        trip_began = False
        trip_terminated = False
        trip_terminated_time = None

        for i, table in enumerate(message_tables):

            # Is the trip present in this table at all?
            if not table[trip_id]:
                # If the trip hasn't been planned yet, and will simply appear in a later trip update, do nothing.
                if not trip_began:
                    pass

                # If the trip has been planned already, and doesn't exist in the current table, then it must have
                # been removed. This implies that this trip terminated in the interceding time. Store this
                # information for later.
                else:
                    trip_terminated = True
                    trip_terminated_time = timestamps[i]

                continue
            else:
                trip_began = True

            action_log = _parse_message_list_into_action_log(table[trip_id], timestamps[i])
            actions_logs.append(action_log)

        trip_log = tripify(actions_logs)

        # Coerce types.
        trip_log = trip_log.assign(
            minimum_time=trip_log.minimum_time.astype('float'),
            maximum_time=trip_log.maximum_time.astype('float'),
            latest_information_time=trip_log.latest_information_time.astype('float')
        )

        # If the trip was terminated sometime in the course of these feeds, update the trip log accordingly.
        if trip_terminated:
            trip_log = _finish_trip(trip_log, trip_terminated_time)

        ret[trip_id] = trip_log

    return ret


def merge_logbooks(logbooks):
    """
    Given a list of trip logbooks (as returned by `parse_feeds_into_trip_logbooks`), returns their merger.
    """
    left = dict()
    for right in logbooks:
        left = _join_logbooks(left, right)
    return left


def _join_logbooks(left, right):
    """
    Given two trip logbooks (as returned by `parse_feeds_into_trip_logbooks`), returns the merger of the two.
    """
    # Figure out what our jobs are by trip id key.
    left_keys = set(left.keys())
    right_keys = set(right.keys())

    mutual_keys = left_keys.intersection(right_keys)
    left_exclusive_keys = left_keys.difference(mutual_keys)
    right_exclusive_keys = right_keys.difference(mutual_keys)

    # Build out non-intersecting trips.
    result = dict()
    for key in left_exclusive_keys:
        result[key] = left[key]
    for key in right_exclusive_keys:
        result[key] = right[key]

    # Build out (join) intersecting trips.
    for key in mutual_keys:
        result[key] = _join_trip_logs(left[key], right[key])

    return result


def _join_trip_logs(left, right):
    """
    Two trip logs may contain information based on action logs, and GTFS-Realtime feed updates, which are
    dis-contiguous in time. In other words, these logs reflect the same trip, but are based on different sets of
    observations.

    In such cases recovering a full(er) record requires merging these two logs together. Here we implement this
    operation.

    This method, the core of merge_trip_logbooks, is an operational necessity, as a day's worth of raw GTFS-R
    messages at minutely resolution eats up 12 GB of RAM or more.
    """
    # Order the frames so that the earlier one is on the left.
    left_start, right_start = left['latest_information_time'].min(), right['latest_information_time'].min()
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

    # Declaring an ordinal categorical column in the stop_id attribute makes `pandas` handle resorting internally and,
    # hence, results in a significant speedup (over doing so ourselves).
    join['stop_id'] = pd.Categorical(join['stop_id'], stations, ordered=True)

    # Update records for stations before the first station in the right trip log that the train is EN_ROUTE_TO or
    # STOPPED_OR_SKIPPED.
    swap_station = right.iloc[0]['stop_id']
    swap_index = next(i for i, station in enumerate(stations) if station == swap_station)
    swap_space = join[:swap_index]
    where_update = swap_space[swap_space['action'] == 'EN_ROUTE_TO'].index.values

    join.loc[where_update, 'action'] = 'STOPPED_OR_SKIPPED'
    join.loc[where_update, 'maximum_time'] = right.loc[0, 'latest_information_time']
    join.loc[swap_index, 'minimum_time'] = left.loc[0, 'minimum_time']

    # Hard-case the columns to float so as to avoid weird typing issues that keep coming up.
    join.loc[:, ['minimum_time', 'maximum_time']] = join.loc[:, ['minimum_time', 'maximum_time']].astype(float)

    # The second trip update may on the first index contain incomplete minimum time information due to not having a
    # reference to a previous trip update included in that trip log's generative action log set. There are a number
    # of ways in which this can occur, but the end fact of the matter is that between the last entry in the first
    # trip log and the first entry in the second trip log, we may have one of three different inconsistencies:
    #
    # 1. The prior states that the train stopped at (or skipped) the last station in that log at some known time,
    #    but the minimum time of the first stop or skip in the posterior log is a NaN, due to lack of prior information.
    # 2. The prior states that the train stopped at (or skipped) the last station in that log at some known minimum
    #    time, but the posterior log first entry minimum time is even earlier.
    # 3. The prior states that the train stopped at (or skipped) the last station in that log at some known maximum
    #    time, but the posterior log first entry minimum time is even earlier.
    #
    # The lines below handle each one of these possible inconsistencies in turn.
    join.loc[:, 'minimum_time'] = join.loc[:, 'minimum_time'].fillna(method='ffill')
    join.loc[1:, 'minimum_time'] = np.maximum.accumulate(join.loc[1:, 'minimum_time'].values)

    if len(join) > 1:
        join.loc[len(left) -1, 'minimum_time'] = np.maximum(np.nan_to_num(join.loc[len(left) - 2, 'maximum_time']),
                                                            join.loc[len(left) - 1, 'minimum_time'])

    # Again at the location of the join, we may also get an incomplete `maximum_time` entry, for the same reason. In
    # this case we will take the `maximum_time` of the following entry. However, note that we are *losing
    # information* in this case, as we could technically resolve this time to a more accurate one, given the full
    # list of information times. However, we do not have that information at this time in the processing sequence.
    # This is an unfortunate but not particularly important, all things considered, technical shortcoming of the way
    # we chose to code things.

    join.loc[:, 'maximum_time'] = join.loc[:, 'maximum_time'].fillna(method='bfill', limit=1)

    return join
