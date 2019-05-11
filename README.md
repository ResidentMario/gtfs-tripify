# gtfs-tripify ![t](https://img.shields.io/badge/status-beta-yellow.svg)

Many major transit municipalities in the United States public realtime information about the state of their systems using a common format known as a [GTFS-Realtime feed](https://developers.google.com/transit/gtfs-realtime/). This is the information that the [Metropolitan Transit Authority](https://en.wikipedia.org/wiki/Metropolitan_Transportation_Authority), for example, uses to power its arrival countdown clocks on station platforms.

`gtfs-tripify` is a Python package for turning streams of GTFS-Realtime messages into a "trip log" of train arrival and departure times. The result is the ground truth history of arrivals and departures of all trains included in the inputted GTFS-RT feeds.

Note that logic for doing so is highly involved, and this library is still under active development. So, expect bugs!

## Quickstart

Begin by running the following to install this package on your local machine:

```sh
pip install gtfs-tripify
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

In addition to a logbook, `gt.logify` also returns two other pieces of information: `timestamps`, a map of unique trip ids to update timestamps (used for merging logbooks); and `parse_errors`, a list of non-fatal errors encountered during the logbook-building process. For more information on possible errors and how they are remediated see the section "Parse errors".

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

## Parse errors

The stream of updates you pass to `gt.logify` may contain any of a large number of non-fatal errors and data inconsistencies, a list of which is returned as part of the method's output. This section documents what they are and how they are handled.

First, some terminology:

* **update** &mdash; A single parsed GTFS-RT update.
* **message** &mdash; An individual entity in an update. There are two kinds: trip update messages, which give schedule information, and vehicle update messages, which give train location information. Complimentary messages are linked by `trip_id`.
* **stream** &mdash; A sequential list of updates over time.

Now for the actual errors:

* `parsing_into_protobuf_raised_exception` &mdash; Occurs when the bytes of a feed update cannot successfully be parsed into a Protobuf. This indicates data corruption. These messages are removed from the field. This will degrade the accuracy of the logbook estimates.
* `parsing_into_protobuf_raised_runtime_warning` &mdash; Occurs when the bytes of a feed update can successfully be parsed into a Protobuf, but doing so raises a `RuntimeWarning`. This likely indicates data loss, and since `gtfs_tripify` is sensitive to such data loss these messages are removed from the feed. This will degrade the accuracy of the logbook estimates.
* `message_with_null_trip_id` &mdash; Occurs when a message in an update in the feed has its `trip_id` set to empty string (`''`). Empty strings are not valid trip identifiers and indicate an error by the feed provider. The offending messages are dropped.
* `trip_has_trip_update_with_no_stops_remaining` &mdash; Occurs when there is a trip update (and optionally a complimentary vehicle update) which has no stops remaining. This is an error by the feed provider, as such trips are supposed to be removed from the feed upon arriving at their final stations. The messages corresponding with this `trip_id` are dropped.
* `trip_id_with_trip_update_but_no_vehicle_update` &mdash; Occurs when there is a trip update with no complimentary vehicle update. This is an error by the feed provider: there is schedule information about a trip but no location information, which makes parsing that schedule impossible. The offending message is dropped.
* `feed_updates_with_duplicate_timestamps` &mdash; Occurs when there are multiple updates in the feed with the same timestamp. This means that either a double read occurred or more likely the feed stopped updating and returned stale data. The offending updates are removed from the field.
* `feed_update_has_null_timestamp` &mdash; Occurs when there is an update has its timestamp set to empty string (`''`) or `0`. These values are null sentinels and indicate an error by the feed provider. The offending update is dropped.
* `feed_update_goes_backwards_in_time` &mdash; Occurs when there is an update in the stream whose timestamp is a smaller value than that of the update immediately prior. This is an error by the feed provider as the stream cannot go backwards in time. The offending update is removed from the feed.

Each entry in `parse_error` includes the `type` of error, taken from the list above, as well as some additional `details` about the error helpful for debugging.

## Further reading

I have written two blog posts about the technical challenges and application posibilities of this library:

* "[Parsing subway rides with gtfs-tripify](https://www.residentmar.io/2018/01/29/gtfs-tripify.html)"
* "[Building an MTA historical train arrival application](https://www.residentmar.io/2018/08/29/subway-explorer.html)"
