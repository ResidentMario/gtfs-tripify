import pandas as pd
import gtfs_tripify as gt
from google.transit import gtfs_realtime_pb2
import warnings
from datetime import datetime


def logbook_to_sql(logbook, conn):
    """
    Write a logbook to a SQL database in a durable manner.
    """
    # Initialize the database.
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS Logbooks (
          "event_id" INTEGER PRIMARY KEY,
          "trip_id" TEXT, "unique_trip_id" INTEGER, "route_id" TEXT, 
          "action" TEXT, "minimum_time" REAL, "maximum_time" REAL,
          "stop_id" TEXT, "latest_information_time" TEXT
        );
    """)
    conn.commit()

    database_unique_ids = set(
        [r[0] for r in c.execute("""SELECT DISTINCT unique_trip_id FROM Logbooks;""").fetchall()]
    )
    root_id_modifier_pairs = set((did[:-2], int(did[-1:])) for did in database_unique_ids)

    # The `trip_id` values included in the GTFS-Realtime streams are not unique. This was the source of much pain
    # in the design of the `gtfs-tripify` library. Furthermore, the modified trip key values used in the trip
    # logs, which are unique within the triplog, are not unique in time: a trip id that gets reused across two
    # different logbooks will be appended a 0 counter in both triplogs, which de-uniquifies the trip when it is
    # written to the database.
    #
    # For the purposes of long-term storage, we must come up with our own unique keys.

    # TODO: Address the Schlemiel the Painter's Algorithm characteristics of this algorithm.
    # We should count up more smartly than just taking step sizes of 1.
    # TODO: Investigate why this algorithm results in such high numbers when used for writing to the database.
    # A typical `unique_trip_id` in the database might be 052800_GS.N03R_792, for no immediately identifiable reason.
    # What the heck?
    key_modifications = {}
    for trip_id in logbook.keys():
        root, mod = trip_id[:-2], int(trip_id[-1:])
        orig_mod = mod
        while True:
            if (root, mod) in root_id_modifier_pairs:
                mod += 1
            else:
                new_mod = mod

                # If the modifier is unchanged, break now---the key is servicable.
                if new_mod == orig_mod:
                    break

                # If it has changed, there has been a collision in the table.
                # Make sure to pick a new key that does not collide with anything in the logbook.
                while True:
                    potential_new_key = "{0}_{1}".format(root, new_mod)

                    if potential_new_key in logbook.keys() or (root, new_mod) in root_id_modifier_pairs:
                        new_mod += 1
                    else:
                        root_id_modifier_pairs.add((root, new_mod))
                        key_modifications["{0}_{1}".format(root, orig_mod)] = "{0}_{1}".format(root, new_mod)
                        break
                break

    for key in key_modifications:
        logbook[key_modifications[key]] = logbook.pop(key)

    # Write out.
    if len(logbook) > 0:
        pd.concat(
            (logbook[trip_id]
                .assign(unique_trip_id=trip_id)
                [['trip_id', 'unique_trip_id', 'route_id', 'action', 'minimum_time', 'maximum_time', 'stop_id',
                  'latest_information_time']]
            ) for trip_id in logbook.keys()
        ).to_sql('Logbooks', conn, if_exists='append', index=False)
        c.close()


def parse_feed(filepath):
    """Helper function for reading a feed in using Protobuf. Handles bad feeds by replacing them with None."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        with open(filepath, "rb") as f:
            try:
                fm = gtfs_realtime_pb2.FeedMessage()
                fm.ParseFromString(f.read())
                return fm
            except (KeyboardInterrupt, SystemExit):
                raise
            # Protobuf occasionally raises an unexpected tag RuntimeWarning. This sometimes occurs when a feed that we
            # read is in an inconsistent state (the other option is a straight-up exception). It's just a warning,
            # but it corresponds with data loss, and `gtfs-tripify` should not be allowed to touch the resulting
            # message --- it will take the non-presence of certain trips no longer present in the database at the
            # given time as evidence of trip ends. We need to explicitly return None for the corresponding messages
            # so they can be totally excised.
            # See https://groups.google.com/forum/#!msg/mtadeveloperresources/9Fb4SLkxBmE/BlmaHWbfw6kJ
            except RuntimeWarning:
                return None
            # TODO: do not use bare except
            except:
                return None


def stream_to_sql(stream, start_time, log_cut_heuristic_exceptions):
    """
    Given a stream of parsed Protobuf messages...
    """
    # TODO: finish ironing out this method's API.
    # TODO: write this method's tests.
    start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")

    stream = [parse_feed(feed) for feed in stream]
    stream = [feed for feed in stream if feed is not None]

    # print("Converting feeds into dictionaries...")
    stream = [gt.dictify(feed) for feed in stream]

    # print("Building the logbook...")
    logbook = gt.logify(stream)
    del stream

    # Cut cancelled and incomplete trips from the logbook. Note that we must exclude shuttles.
    # print("Trimming cancelled and incomplete stops...")
    for trip_id in logbook.keys():
        if len(logbook[trip_id]) > 0 and logbook[trip_id].iloc[0].route_id not in log_cut_heuristic_exceptions:
            logbook[trip_id] = gt.utils.cut_cancellations(logbook[trip_id])

    logbook = gt.utils.discard_partial_logs(logbook)

    # Cut empty trips, singleton trips, and trips that began on the follow-on day.
    # print("Cutting cancelled and follow-on-day trips...")
    trim = logbook.copy()
    for trip_id in logbook.keys():
        if len(logbook[trip_id]) <= 1:
            del trim[trip_id]
        else:
            start_ts = logbook[trip_id].iloc[0]['latest_information_time']
            if datetime.fromtimestamp(int(start_ts)).day != start_datetime.day:
                del trim[trip_id]

    del logbook

    print("Writing to SQL...")
    gt.utils.to_sql(trim, conn)
