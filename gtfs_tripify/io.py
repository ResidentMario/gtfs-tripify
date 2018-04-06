import pandas as pd
import gtfs_tripify as gt
import warnings

# This module will only work if the Google parser is provided, but we do not want to make it a package dependency.
try:
    from google.transit import gtfs_realtime_pb2
except ImportError:
    pass


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
);""")
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
    # TODO: tests.
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        with open(filepath, "rb") as f:
            try:
                fm = gtfs_realtime_pb2.FeedMessage()
                fm.ParseFromString(f.read())
                return fm

            # Protobuf occasionally raises an unexpected tag RuntimeWarning. This occurs when a feed that we
            # read has unexpected problems, but is still valid overall. This warning corresponds with data loss in
            # most cases. `gtfs-tripify` is sensitive to the disappearance of trips in the record. If data is lost,
            # it's best to excise the message entirely. Hence we catch these warnings and return a flag value None,
            # to be taken into account upstream. For further information see the following thread:
            # https://groups.google.com/forum/#!msg/mtadeveloperresources/9Fb4SLkxBmE/BlmaHWbfw6kJ
            except RuntimeWarning:
                return None

            # Raise for system and user interrupt signals.
            except (KeyboardInterrupt, SystemExit):
                raise

            # Return the same None flag value for all other (Protobuf-thrown) errors.
            # TODO: do not use bare except.
            except:
                return None


def stream_to_sql(stream, conn, transform=None):
    """
    Write the logbook generated from a parsed Protobuf stream into a SQL database in a durable manner. To transform
    the data in the logbook before writing to the database, provide a method doing so to the `transform` parameter.
    """
    stream = [parse_feed(feed) for feed in stream]
    stream = [feed for feed in stream if feed is not None]
    stream = [gt.dictify(feed) for feed in stream]

    logbook = gt.logify(stream)
    del stream

    if transform:
        logbook = transform(logbook)

    gt.io.logbook_to_sql(logbook, conn)
