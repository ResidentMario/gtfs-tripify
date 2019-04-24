# gtfs-tripify ![t](https://img.shields.io/badge/status-alpha-red.svg)

Many major transit municipalities in the United States public realtime information about the state of their systems using a common format known as a [GTFS-Realtime feed](https://developers.google.com/transit/gtfs-realtime/). This is the information that the [Metropolitan Transit Authority](https://en.wikipedia.org/wiki/Metropolitan_Transportation_Authority), for example, uses to power its arrival countdown clocks on station platforms.

`gtfs-tripify` is a Python package for turning streams of GTFS-Realtime messages into a "trip log" of train arrival and departure times. The result is the ground truth history of arrivals and departures of all trains included in the inputted GTFS-RT feeds.

## Quickstart

Begin by running the following to install this package on your local machine:

```sh
pip install git+git://github.com/ResidentMario/gtfs-tripify.git@master
```

First we need to prepare our GTFS-Realtime feeds of interest. GTFS-Realtime is a highly compressed binary format encoded using a Google data encoding known as Protobuf. The easiest way to access the data is to use the default decoder Google has written for us, the [`gtfs_realtime_bindings` package](https://github.com/google/gtfs-realtime-bindings/tree/master/python). That's what we do below:

```python
# Load GTFS-Realtime feeds.
# For this example we will use publicly archived MTA data.
import requests
response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')

# Load a GTFS-Realtime parser. We use the default Google parser.
# cf. https://github.com/google/gtfs-realtime-bindings/tree/master/python
from google.transit import gtfs_realtime_pb2

# Build an example message stream.
message1 = gtfs_realtime_pb2.FeedMessage()
message1.ParseFromString(response1.content)
message2 = gtfs_realtime_pb2.FeedMessage()
message2.ParseFromString(response2.content)
message3 = gtfs_realtime_pb2.FeedMessage()
message3.ParseFromString(response3.content)
stream = [message1, message2, message3]
```

Now we have a bunch of `gtfs_realtime_pb2.FeedMessage` object, each of which is a single decompressed GTFS-Realtime feed message (or just "message" for short). Each of these feeds represents the state of the same wired-up slice of the MTA transit network at a different but consecutive point in time.

This is where `gtfs_tripify` comes in:

```python
import gtfs_tripify as gt
logbook, logbook_timestamps = gt.logify(stream)
```

Now we have a `logbook`. If we inspect it we see that it is a `dict` with the following format:

```python
{
    '87a19e7a-66dd-11e9-b1fe-8c8590adc94b': <pandas.DataFrame object>,
    '87a19db4-66dd-11e9-a0e0-8c8590adc94b': <pandas.DataFrame object>,
    ...
}
```

Each entry in the `logbook` is a `log`. Each log provides information about a single train trip in the system.

```python
print(logbook['87a19e7a-66dd-11e9-b1fe-8c8590adc94b'])
```

This looks something like this:

```python

          trip_id route_id              action minimum_time maximum_time  \
0  047850_2..S05R        2  STOPPED_OR_SKIPPED   1410960621   1410961221
1  047850_2..S05R        2  STOPPED_OR_SKIPPED   1410960621   1410961221
2  047850_2..S05R        2          STOPPED_AT   1410960621          nan
3  047850_2..S05R        2         EN_ROUTE_TO   1410961221          nan
4  047850_2..S05R        2         EN_ROUTE_TO   1410961221          nan
5  047850_2..S05R        2         EN_ROUTE_TO   1410961221          nan
6  047850_2..S05R        2         EN_ROUTE_TO   1410961221          nan
7  047850_2..S05R        2         EN_ROUTE_TO   1410961221          nan
8  047850_2..S05R        2         EN_ROUTE_TO   1410961221          nan

  stop_id latest_information_time
0    238S              1410961221
1    239S              1410961221
2    241S              1410961221
3    242S              1410961221
4    243S              1410961221
5    244S              1410961221
6    245S              1410961221
7    246S              1410961221
8    247S              1410961221
```

Note that some of the resulting columns are references to fields in the companion [GTFS Feed](https://developers.google.com/transit/gtfs/), basically a packet of `csv` files explaning how the system is laid out.

Values are:

* `trip_id`: The ID assigned to the trip in the GTFS-Realtime record.
* `route_id`: The ID of the route. In New York City these are easy to read: 2 means this is a number 2 train.
* `stop_id`: The ID assigned to the stop in question.
* `action`: The action that the given train took at the given stop. One of `STOPPED_AT`, `STOPPED_OR_SKIPPED`, or `EN_ROUTE_TO` (the latter only occurs if the trip is still in progress).
* `minimum_time`: The minimum time at which the train pulled into the station. May be `NaN`. This time is a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
* `maximum_time`: The maximum time at which the train pulled out of the station. May be `NaN`. Also a Unix timestamp.
* `latest_information_time`: The timestamp of the most recent GTFS-Realtime data feed containing information pertinent to this record. Also a Unix timestamp.

## Additional methods

`gtfs_tripify` will by default provide as much information as possible, and will include both incomplete trips (trips which are still in progress as of the last message in the stream) and cancelled stops (stops that did not occur due to trip cancellations). You may prune these from the logbook using `gt.utils.cut_cancellations` and `gt.utils.discard_partial_logs`, respectively:

```python
len(logbook)  # 313 logs included
sum(len(log) for log in logbook)  # 11268 log entries included

logbook = gt.utils.cut_cancellations(logbook)
logbook = gt.utils.discard_partial_logs(logbook)

len(logbook)  # 245 logs remaining
sum(len(log) for log in logbook)  # 8820 log entries remaining
```

Each logbook naturaly represents all stops in a system for a specific "time slice". You can construct a larger logbook out of smaller ones using `gt.merge_logbooks([(logbook_1, logbook_1_timestamps), (logbook_2, logbook_2_timestamps), ...])`. Note that the logbooks must be in *sorted contiguous order*. Each logbook must address adjacent time slices, e.g. `[1:00, 1:59], [2:00, 2:59], [3:00, 3:59], ...`.

Use the `gt.io.logbooks_to_sql` or `gt.io.stream_to_sql` helper methods to persist the data to a SQLite database. **Note**: these methods are currently under renovation.

## Further reading

I have written two blog posts about the technical challenges and application posibilities of this library:

* "[Parsing subway rides with gtfs-tripify](https://www.residentmar.io/2018/01/29/gtfs-tripify.html)"
* "[Building an MTA historical train arrival application](https://www.residentmar.io/2018/08/29/subway-explorer.html)"