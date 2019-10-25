"""
Library utility functions.
"""

import itertools
import tarfile
import os
import datetime

import numpy as np
import requests


def synthesize_route(station_lists):
    """
    Given a list of station lists (that is: a list of lists, where each sublist consists of the
    series of stations which a train was purported to be heading towards at any one time), 
    returns the synthetic route of all of the stops that train may have stopped at, in the order
    in which those stops would have occurred.
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
        # ...then the stations that appear before the pivot in the first list, the pivot, and 
        # the stations that appear after the pivot in the second list should be the ones that 
        # are included
        return (left[:pivot_left] +
                [s for s in right[:pivot_right] if s not in left[:pivot_left]] +
                right[pivot_right:])
    # If we did not find a pivot...
    else:
        # ...then none of the stations that appear in the second list appeared in the first 
        # list. This means that the train probably cancelled those stations, but it may have 
        # stopped there in the meantime also. Add all stations in the first list and all 
        # stations in the second list together.
        return left + right


def finish_trip(trip_log, timestamp):
    """
    Finishes a trip. We know a trip is finished when its messages stops appearing in feed files,
    at which time we can "cross out" any stations still remaining.
    """
    trip_log = (trip_log.replace('EN_ROUTE_TO', 'STOPPED_OR_SKIPPED')
                        .replace('EXPECTED_TO_SKIP', 'STOPPED_OR_SKIPPED')
                        .replace('nan', np.nan))
    trip_log['maximum_time'] = trip_log['maximum_time'].fillna(timestamp)
    return trip_log


# TODO: use a datetime as input instead of a string, as in `load_mytransit_archived_feeds`
def load_mta_archived_feed(feed='gtfs', timestamp='2014-09-17-09-31'):
    """
    Returns archived GTFS data for a particular time_assigned.

    Parameters
    ----------
    feed: {'gtfs', 'gtfs-l', 'gtfs-si'}
        Archival data is provided in these three rollups. The first one covers 1-6 and the S, 
        the second covers the L, and the third, the Staten Island Railway.
    timestamp: str
        The time_assigned associated with the data rollup. The files are time stamped at 01, 
        06, 11, 16, 21, 26, 31, 36, 41, 46, 51, and 56 minutes after the hour, so only these 
        times will be valid.
    """
    return requests.get(
        "https://datamine-history.s3.amazonaws.com/{0}-{1}".format(feed, timestamp)
    )


def load_mytransit_archived_feeds(timestamp=datetime.datetime(2017, 1, 1, 12, 0)):
    """
    Given a timestamp, loads a roundup of minutely MTA feeds for that day. The data is returned 
    as a list of `ExFileObject` virtual files (use `read` to get raw bytes).

    This data is loaded from Nathan Johnson's data.transit.nyc archiving project 
    (http://data.mytransit.nyc/). His archive is a complete record of the data from January 31st, 
    2016 through May 31st, 2017.
    """
    ts = timestamp
    uri = (
        f"http://data.mytransit.nyc.s3.amazonaws.com/subway_time/{ts.year}/{ts.year}-"
        f"{str(ts.month).rjust(2, '0')}/subway_time_"
        f"{str(ts.year) + str(ts.month).rjust(2, '0') + str(ts.day).rjust(2, '0')}.tar.xz"
    )

    # The tar module does not seem to support reading virtual files via io.BytesIO, we have to go 
    # to disc.
    temp_filename = "temp.tar.xz"
    with open(temp_filename, "wb") as f:
        f.write(requests.get(uri).content)

    archive = tarfile.open(temp_filename, 'r')
    names = [member.name for member in archive.getmembers()]
    messages = [archive.extractfile(f) for f in archive.getmembers()]
    os.remove(temp_filename)

    return messages, names

__all__ = [
    'synthesize_route', 'finish_trip', 'load_mta_archived_feed', 'load_mytransit_archived_feeds'
]
