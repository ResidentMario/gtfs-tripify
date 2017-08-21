import numpy as np
import itertools
from collections import defaultdict
import pandas as pd
from gtfs_tripify.utils import synthesize_route


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

    return feed


def correct(feed):
    """
    Verifies that the inputted dictified feed has the expected schema. Raises warnings wherever issues are found,
    and attempts to cure them.
    """
    # TODO
    return feed


def tripsort(feed, include_alerts=False):
    """
    Sorts the messages a set of dictified feeds into a hash table.
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

        # Intermediate station, one of arrival or departure is null.
        elif ((not first_station and
               not last_station and
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
    all_data = pd.concat(tripwise_action_logs)

    key_data = all_data.groupby('information_time').first().reset_index()
    current_information_time = None

    # The following bookkeeping is used to assign the *next* information time in the case of a STOPPED_AT.
    information_times = sorted(list(set(all_data['information_time'])))
    next_information_time_index = 1
    next_information_time = information_times[1] if len(information_times) > 1 else np.nan

    # To understand what went on during a trip, we only need to have a list of touched stops, the rows corresponding
    # with the first action in each observation's action sublog, and the time that has passed in between the sublog
    # entries. We can extract all of the stop information that we need by considering information pertaining to
    # these entries, in order.
    station_lists = []
    for log in tripwise_action_logs:
        station_lists.append(list(log['stop_id'].unique()))
    remaining_stops = synthesize_route(station_lists)

    # Base is trip_id, route_id.
    base = np.array([all_data.iloc[0]['trip_id'], all_data.iloc[0]['route_id']])

    lines = []

    for ind, row in key_data.iterrows():

        previous_information_time = current_information_time if current_information_time is not None else np.nan
        current_information_time = row['information_time']

        # Do bookkeeping to keep track of the next information time for use by STOPPED_AT records.
        if current_information_time == next_information_time:
            next_information_time_index += 1
            try:
                next_information_time = information_times[next_information_time_index]
            except IndexError:  # end of the record
                next_information_time = np.nan

        current_stop = row['stop_id']

        i_del = 0
        for remaining_stop in remaining_stops:
            if remaining_stop != current_stop:
                # action, minimum_time, maximum_time, stop_id, latest_information_time
                skipped_stop = np.append(base.copy(), np.array(
                    ['STOPPED_OR_SKIPPED', previous_information_time, current_information_time,
                     remaining_stop, current_information_time]
                ))
                lines.append(skipped_stop)
                i_del += 1
            else:
                if row['action'] == 'STOPPED_AT':
                    stopped_stop = np.append(base.copy(), np.array(
                        ['STOPPED_AT', previous_information_time, next_information_time,
                         row['stop_id'], current_information_time]
                    ))
                    lines.append(stopped_stop)
                    i_del += 1
                    break
                else:
                    # We have learned nothing.
                    break
        remaining_stops = remaining_stops[i_del:]

    # Any stops left over we haven't arrived at yet.
    for remaining_stop in remaining_stops:
        future_stop = np.append(base.copy(), np.array(
            ['EN_ROUTE_TO', current_information_time, np.nan,
             remaining_stop, current_information_time]
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
    message_tables = [tripsort(feed) for feed in feeds]
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
        ret[trip_id] = trip_log

        # If the trip was terminated sometime in the course of these feeds, update the trip log accordingly.
        if trip_terminated:
            ret[trip_id] = _finish_trip(ret[trip_id], trip_terminated_time)

    return ret
