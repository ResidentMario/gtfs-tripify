# gtfs-tripify ![t](https://img.shields.io/badge/status-alpha-red.svg)

`gtfs-tripify` is a Python package for creating trip logs out of GTFS-Realtime messages.

## Quickstart

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
feed. Each of these feeds represents the state of the same wired-up slice of the MTA transit network at different but
consequetive points in time.

Before we process them however, let's transform these objects into `dict` objects. Dictionaries have a >10x smaller 
memory footprint than `gtfs_realtime_pb2.FeedMessage` objects. The following transform also makes the message conform
 to our schema by tossing or massaging out errors it encounters!

```python
from gtfs_tripify import dictify
feeds = [dictify(feed) for feed in [feed1, feed2, feed3]]
```

Now we are ready to run this through logbuilding.

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

Each of the dictionary keys is a unique ID assigned to a particular trip. This unique ID is **not** the same as the 
`trip_id` assigned to the trip in question in the raw GTFS-Realtime feed. The reason for this is that the MTA only 
guarantees that `trip_id` is unique in the feed it appears in. However, when a trip ends, that trip's ID is released 
and recycled for the next trip added to the record. `gtfs-tripify` works around this problem by appending a number, 
`_0` in these two cases, to the very end of that trip's `trip_id`. When the ID is recycled, the trips further into 
the future pick up higher numbers.

So for example, suppose that a trip with the ID `047850_2..S05R` runs at 12:00 PM today. The trip ends at 2:00 PM. At
 4:00 PM a new trip is added to the record, again with the ID `047850_2..S05R`! In this case the first trip will be 
 labeled `047850_2..S05R_0` and the second will be labeled `047850_2..S05R_1`, and so on.

The contents of each trip label is a `trip log`.

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

With that in mind, the fields are:

* `trip_id`: The ID assigned to the trip in the GTFS-Realtime record. This ID is guaranteed to be unique within a given
 feed, but is almost guaranteed to be non-unique historically. Hence the business with the keys explained above!
* `route_id`: The ID of the route. This is a reference to the ID given to a particular line in `routes.csv` of the 
complementary GTFS record. In the New York City case, these IDs are easy to read: 2 means this is a number 2 train.
* `stop_id`: The ID assigned to the stop un question, as given in the complimentary `stops.csv` file. `N` means this 
is a northbound train stop, while `S` means it is a southbound one.
* `action`: The action that the given train took at the given stop. One of `STOPPED_AT`, `STOPPED_OR_SKIPPED`, or 
`EN_ROUTE_TO` (if the trip is still in progress).
* `minimum_time`: The minimum time at which the train pulled into the station. May be `NaN`. This time is a [Unix 
timestamp](https://en.wikipedia.org/wiki/Unix_time).
* `maximum_time`: The maximum time at which the train pulled out of the station. May be `NaN`. Also a Unix timestamp.
* `latest_information_time`: The timestamp of the most recent GTFS-Realtime data feed containing information 
pertinent to this record. Also a Unix timestamp.

And that's all you need to know to put this data to use! Go frollick!

## Background

The MTA, the local subway service in New York City, has been receiving an ever-increasing battering in the press for 
the last whenever for constant delays and slow service on the city's train lines (see e.g. [this piece](https://www.villagevoice.com/2017/08/02/subway-summer-of-hell-really-started-years-ago-data-shows/) in the Village Voice). 
Journalistic publications have indited a variety of factors. The Voice piece draws a good point on this subject: 
that all of the various blame games have occurred "absent objective data".

The MTA publishes such objective data, actually, in the form of what is known as a GTFS-Realtime feed. This is a 
realtime data format that was invented over at Google for the purposes of providing reliable rapid transit updates.
It's the one that powers transit information in applications like Google Maps and the various subway tracking 
applications on the App Store. It also powers the arrival time update kiosks and panels that have begun to penetrate 
MTA train stations as of late.

However, GTFS-Realtime data is a format that, albeit good for telling you when your next train will arrive, makes 
reconstructing a history *of* that train very difficult.

Since I was interested in injecting some of the sought-after "objective data" into the story being told about the 
MTA, I decided to tackle the challenge of transforming GTFS-Realtime feeds into reconstructed trip data (in what I 
call "trip logs"). It was a mountain of a challenge. It's done now.

However, I have not, as of yet, actually *used* this package for anything. The problem with ambitious ETL 
transformations like this one is that it's hard to judge the value of the result you get until you get it. And the 
result we get is still not quite good enough...

The MTA system is right bloody complicated. I succeeded in isolating stop sequences in time, however I did not do any
work to further that into isolating route sequences. This is necessary in part because trains can get rerouted onto 
different lines, and in part because individual lines can run any of a number of different *routes* depending on the
weekday, time of day, holiday schedule, alignment of the moons of Saturn, etecetera.

Isolating that stuff, too, would require a whole second order of logic: not something I have the time to do right 
now. But maybe you do?
