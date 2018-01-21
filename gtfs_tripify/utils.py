import datetime
import numpy as np
import itertools
import tarfile
import os
import pandas as pd


def synthesize_route(station_lists):
    """
    Given a list of station lists (that is: a list of lists, where each sublist consists of the series of stations
    which a train was purported to be heading towards at any one time), returns the synthetic route of all of the
    stops that train may have stopped at, in the order in which those stops would have occurred.
    """
    ret = []
    for i in range(len(station_lists)):
        ret = _synthesize_station_lists(ret, station_lists[i])
    return ret


def _synthesize_station_lists(left, right):
    """
    Pairwise synthesis op. Submethod of the above.
    """
    # First, find the pivot.
    pivot_left = pivot_right = -1
    for j in range(len(left)):
        station_a = left[j]
        for k in range(len(right)):
            station_b = right[k]
            if station_a == station_b:
                pivot_left = j
                pivot_right = k
                break

    # If we found a pivot...
    if pivot_left != -1:
        # ...then the stations that appear before the pivot in the first list, the pivot, and the stations that
        # appear after the pivot in the second list should be the ones that are included
        return (left[:pivot_left] +
                [s for s in right[:pivot_right] if s not in left[:pivot_left]] +
                right[pivot_right:])
    # If we did not find a pivot...
    else:
        # ...then none of the stations that appear in the second list appeared in the first list. This means that the
        #  train probably cancelled those stations, but it may have stopped there in the meantime also. Add all
        # stations in the first list and all stations in the second list together.
        return left + right


def load_mta_archived_feed(feed='gtfs', timestamp='2014-09-17-09-31'):
    """
    Returns archived GTFS data for a particular time_assigned.

    Parameters
    ----------
    feed: {'gtfs', 'gtfs-l', 'gtfs-si'}
        Archival data is provided in these three rollups. The first one covers 1-6 and the S, the second covers the
        L, and the third, the Staten Island Railway.
    timestamp: str
        The time_assigned associated with the data rollup. The files are time stamped at 01, 06, 11, 16, 21, 26, 31, 36,
        41, 46, 51, and 56 minutes after the hour, so only these times will be valid.
    """
    import requests

    return requests.get("https://datamine-history.s3.amazonaws.com/{0}-{1}".format(feed, timestamp))


def load_mytransit_archived_feeds(timestamp=datetime.datetime(2017, 1, 1, 12, 0)):
    """
    Given a timestamp, loads a roundup of minutely MTA feeds for that day. The data is returned as a list of
    `ExFileObject` virtual files (use `read` to get raw bytes).

    This data is loaded from Nathan Johnson's data.transit.nyc archiving project (http://data.mytransit.nyc/). His
    archive is a complete record of the data from January 31st, 2016 through May 31st, 2017.
    """
    import requests

    ts = timestamp
    uri = "http://data.mytransit.nyc.s3.amazonaws.com/subway_time/{0}/{0}-{1}/subway_time_{2}.tar.xz".format(
        ts.year, str(ts.month).rjust(2, '0'), str(ts.year) + str(ts.month).rjust(2, '0') + str(ts.day).rjust(2, '0')
    )
    # filename_date_format = str(datetime.datetime.strftime(datetime.datetime(2016, 1, 1), "%Y%m%dT%H%MZ"))

    # The tar module does not seem to support reading virtual files via io.BytesIO, we have to go to disc.
    temp_filename = "temp.tar.xz"
    with open(temp_filename, "wb") as f:
        f.write(requests.get(uri).content)

    archive = tarfile.open(temp_filename, 'r')
    messages = [archive.extractfile(f) for f in archive.getmembers()]
    os.remove(temp_filename)

    return messages


##############
# HEURISTICS #
##############

def cut_cancellations(log):
    """
    Heuristically cuts stops that almost certainly didn't happen do to trip cancellations. I refer to this as
    the "cut-cancellation" heuristic.

    Returns a minified log containing only trips that almost assuredly happened.
    """
    # Immediately return if the log is empty.
    if len(log) == 0:
        return log
    # Heuristically return an empty log if there are zero confirmed stops in the log.
    elif ~(log.action == 'STOPPED_AT').any():
        return log.head(0)
    # Heuristically cut len >= 2 `STOPPED_OR_SKIPPED` blocks with the same `LATEST_INFORMATION_TIME`.
    else:
        # Find the last definite stop.
        last_definite_stop = np.argmax(log.action[::-1] == 'STOPPED_AT')
        suspicious_block = log.tail(-last_definite_stop - 1)
        if len(suspicious_block) == 1:
            return log
        elif len(suspicious_block['latest_information_time'].unique()) == 1:
            return log.head(last_definite_stop + 1)
        else:
            return log


def discard_partial_logs(logbook):
    """
    Discards logs which appear in the first or last message in the feed. These logs are extremely likely to be
    partial because we do not get to "see" every single message corresponding with the trip, as some are outside our
    "viewing window".
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


######
# IO #
######

def to_sql(logbook, conn):
    """
    Write a logbook to a SQL database in a durable manner.
    """
    # Initialize the database.
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS Logbooks (
          "index" INTEGER PRIMARY KEY,
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
