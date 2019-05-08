# gtfs-tripify ![t](https://img.shields.io/badge/status-beta-yellow.svg)

Many major transit municipalities in the United States public realtime information about the state of their systems using a common format known as a [GTFS-Realtime feed](https://developers.google.com/transit/gtfs-realtime/). This is the information that the [Metropolitan Transit Authority](https://en.wikipedia.org/wiki/Metropolitan_Transportation_Authority), for example, uses to power its arrival countdown clocks on station platforms.

`gtfs-tripify` is a Python package for turning streams of GTFS-Realtime messages into a "trip log" of train arrival and departure times. The result is the ground truth history of arrivals and departures of all trains included in the inputted GTFS-RT feeds.

Note that logic for doing so is highly involved, and this library is still under active development. So, expect bugs!

## Quickstart

Begin by running the following to install this package on your local machine:

```sh
pip install git+git://github.com/ResidentMario/gtfs-tripify.git@master
```

First we need to prepare our GTFS-Realtime feeds of interest. GTFS-Realtime is a highly compressed binary format encoded using a Google data encoding known as Protobuf.

```python
# Load GTFS-Realtime feeds.
# For this example we will use publicly archived MTA data.
import requests
response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')

stream = [response1.content, response2.content, response3.content]
```

We now have the raw bytes for a sequence of GTFS-Realtime feed updates. Each update represents the state of the same wired-up slice of a transit network at a different but consecutive point in time.

This is where `gtfs_tripify` comes in:

```python
import gtfs_tripify as gt
logbook, timestamps, parse_errors = gt.logify(stream)
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

The `ops` submodue contains a variety of operations useful for working with logbooks.

`gtfs_tripify` will by default provide as much information as possible, and will include both incomplete trips (trips which are still in progress as of the last message in the stream) and cancelled stops (stops that did not occur due to trip cancellations). You may prune these:

```python
len(logbook)  # 313 logs included
sum(len(log) for log in logbook)  # 11268 log entries included

pruned_logbook = gt.ops.cut_cancellations(pruned_logbook)
pruned_logbook = gt.ops.discard_partial_logs(pruned_logbook)

len(pruned_logbook)  # 245 logs remaining
sum(len(log) for log in pruned_logbook)  # 8820 log entries remaining
```

You may partition a logbook into complete and incomplete trip logbooks:

```python
complete_logbook, complete_timestamps, incomplete_logbook, incomplete_timestamps =\
    gt.ops.partition_on_incomplete(logbook, timestamps)
```

Or partition a logbook based on route:

```python
logbooks_by_route, timestamps_by_route = gt.ops.partition_on_route(logbook, timestamps)
```

You can construct a larger logbook out of a contiguous sequence of smaller ones:

```python
old_logbook, old_timestamps = logbook, timestamps

response4 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')
response5 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-51')
response6 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')

new_stream = [response4.content, response5.content, response6.content]
new_logbook, new_timestamps = gt.logify(new_stream)

combined_logbook, combined_logbook_timestamps = gt.ops.merge(
    [(old_logbook, old_timestamps), (new_logbook, new_timestamps)]
)
```

**Note**: the `trip_id` field in a GTFS-RT feed may be reassigned to a new train mid-trip and without warning. `gt.logify` can catch and correct this in many (but not all!) cases, `gt.ops.merge` cannot and, in the case that the reassignment happens to occur in the space in between two logbooks, will record two separate partial trips instead. So it's highly recommended to only merge large logbooks, to help avoid "trip fragmentation".

Finally, you may save a logbook to disk. There are a couple of methods for doing so: `gt.ops.to_csv` (and its companion `gt.ops.from_csv`), which will write a logbook to disk as a CSV file, and `gt.ops.to_gtfs`, which will write a logbook to disk as a GTFS `stop.txt` record. You should only use `gt.ops.to_gfst` on complete logbooks (e.g., ones which you have run `gt.ops.cut_cancellations` and `gt.ops.discard_partial_logs` on), as the GTFS spec allows neither null values nor hypothetical stops in `stops.txt`, so the offending stop records will be ignored.

## Further reading

I have written two blog posts about the technical challenges and application posibilities of this library:

* "[Parsing subway rides with gtfs-tripify](https://www.residentmar.io/2018/01/29/gtfs-tripify.html)"
* "[Building an MTA historical train arrival application](https://www.residentmar.io/2018/08/29/subway-explorer.html)"