# gtfs-tripify ![t](https://img.shields.io/badge/status-alpha-red.svg)

The [Metropolitan Transit Authority](https://en.wikipedia.org/wiki/Metropolitan_Transportation_Authority) is the 
primary public transportation authority for the greater New York City region. It provides real-time information about 
its buses, subway trains, and track trains using a bundle of what are called [GTFS-Realtime 
feeds](https://developers.google.com/transit/gtfs-realtime/). Each GTFS-RT feed represents a snapshot of a slice of the 
MTA's service jurisdiction at a certain timestamp.

`gtfs-tripify` is a Python package for turning streams of GTFS-Realtime messages into a "trip log" of train arrival and 
departure times. The result is the ground truth history of arrivals and departures of all trains included in the 
inputted GTFS-RT feeds.

[For more on how this package came to be, read this blog post](http://www.residentmar.io/2018/01/29/gtfs-tripify.html).

## Quickstart

Begin by running the following to install this package on your local machine:

```sh
pip install git+git://github.com/ResidentMario/gtfs-tripify.git@master
```

First we need to prepare our GTFS-Realtime feeds of interest. GTFS-Realtime is a highly compressed binary format 
encoded using a Google data encoding known as Protobuf. The easiest way to access the data is to use the default 
decoder Google has written for us, the [`gtfs_realtime_bindings` package](https://github.com/google/gtfs-realtime-bindings/tree/master/python). That's what we do below:

```python
# Load GTFS-Realtime feeds. We use archived MTA data.
import requests
response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')

# Load a GTFS-Realtime parser.
# We use the default Google binding they provide.
# cf. https://github.com/google/gtfs-realtime-bindings/tree/master/python
from google.transit import gtfs_realtime_pb2
feed1 = gtfs_realtime_pb2.FeedMessage()
feed1.ParseFromString(response1.content)
feed2 = gtfs_realtime_pb2.FeedMessage()
feed2.ParseFromString(response3.content)
feed3 = gtfs_realtime_pb2.FeedMessage()
feed3.ParseFromString(response3.content)
```

Now we have a bunch of `gtfs_realtime_pb2.FeedMessage` object, each of which is a single decompressed GTFS-Realtime 
feed message (or just "message" for short). Each of these feeds represents the state of the same wired-up slice of the MTA transit network at a different but consequetive point in time.

This is where `gtfs_tripify` comes in. The `dictify` method can be used to transform these results in Python dictionaries (`dict` objects). A `dict` is less than a tenth the size of a `gtfs_realtime_pb2.FeedMessage` object. This transformation also repairs any errors it finds in the feed.

```python
from gtfs_tripify import dictify
feeds = [dictify(feed) for feed in [feed1, feed2, feed3]]
```

Now it's time to build a logbook. We do this with the `logify` method.

```python
from gtfs_tripify import logify
logbook = logify(feeds)
```

At this point we have a `logbook`. If we inspect it we see that it is a `dict` with the following format:

```python
{
    '047850_2..S05R_0': <pandas.DataFrame object>,
    '051350_2..N01R_0': <pandas.DataFrame object>,
    [...]
}
```

Each of the dictionary keys is a unique ID assigned to a particular trip. The contents of each trip label is a `trip log`.

```python
print(logbook['047850_2..S05R_0'])
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

Before explaining the fields, I need to point out that some of them are references to fields in what is known as the 
GTFS record. The GTFS record is a separate body of data published alongside a GTFS-Realtime stream. It's basically 
just a big packet of CSV files which explain how the system runs: what `stop_id` corresponds to what physical stop, 
what the names of the routes are, what the variants in service are, etecetera.

The fields are:

* `trip_id`: The ID assigned to the trip in the GTFS-Realtime record.
* `route_id`: The ID of the route. This is a reference to the ID given to a particular line in `routes.csv` of the 
complementary GTFS record. In the New York City case, these IDs are easy to read: 2 means this is a number 2 train.
* `stop_id`: The ID assigned to the stop in question, as given in the complimentary `stops.csv` file. `N` means this 
is a northbound train stop, while `S` means it is a southbound one.
* `action`: The action that the given train took at the given stop. One of `STOPPED_AT`, `STOPPED_OR_SKIPPED`, or 
`EN_ROUTE_TO` (if the trip is still in progress).
* `minimum_time`: The minimum time at which the train pulled into the station. May be `NaN`. This time is a [Unix 
timestamp](https://en.wikipedia.org/wiki/Unix_time).
* `maximum_time`: The maximum time at which the train pulled out of the station. May be `NaN`. Also a Unix timestamp.
* `latest_information_time`: The timestamp of the most recent GTFS-Realtime data feed containing information 
pertinent to this record. Also a Unix timestamp.

At this point you have all of the stop data you could get, and may use it as you see fit. However, there are a couple of additional methods you may want to use.

If you want *only* trips which are complete, not ones that are in progress, you may use the `gtfs_tripify.utils.discard_partial_logs` method to trim trips that are still en route to their final destination.

By default, I don't remove stops that did not occur due to the trip being cancelled. These occur very often because it's relatively common for a train trip to be cancelled, and for the train in question to be reassigned a different trip. An end-to-end service run may break down into many trips. Use the `gtfs_tripify.utils.discard_partial_logs` to get rid of these. Doing so is highly recommended. Just don't try it with two-stop shuttle services!

## Gotchas

* Train trips are often partial. In fact, it's relatively common for a train trip to be cancelled, and for the train in question to be reassigned to a new and different trip plan. This can happen arbitrarily many times in one complete service run. In other words, one complete end-to-end service run (from the first station to the last) may be composed of two, three, or even more distinct trips!

* The unique ID used as the logbook key is **not** the same as the `trip_id` assigned to the trip in question in the raw GTFS-Realtime feed. The reason for this is that the MTA only guarantees that `trip_id` is unique in the feed it appears in. However, when a trip ends, that trip's ID is released and recycled for the next trip added to the record. `gtfs-tripify` works around this problem by appending a number, `_0` in these two cases, to the very end of that trip's `trip_id`. When the ID is recycled, the trips further into the future pick up higher numbers.

  So for example, suppose that a trip with the ID `047850_2..S05R` runs at 12:00 PM today. The trip ends at 2:00 PM. At 4:00 PM a new trip is added to the record, again with the ID `047850_2..S05R`! In this case the first trip will be labeled `047850_2..S05R_0` and the second will be labeled `047850_2..S05R_1`, and so on.
  
  `gtfs_tripify` works around this problem by marking off when trip IDs dissappear from the feed. It is theoretically possible for a trip ID to be reassigned within the interval between two messages. There is no naive way to detect when this occurs, and `gtfs_tripify` doesn't even try.
  
* Sometimes feed messages are returned in a corrupted state. `gtfs_realtime_pb2` will fail to load these completely. You will lose some information in the output `logbook` (as there will be a larger gap between these messages), but `gtfs_tripify` is able to handle variably spaced GTFS-R messages.
