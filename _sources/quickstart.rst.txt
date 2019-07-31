Quickstart
==========

Begin by running the following to install this package on your local
machine:

.. code:: sh

   pip install gtfs_tripify

First we need to prepare our GTFS-Realtime feeds of interest.
GTFS-Realtime is a highly compressed binary format encoded using a
Google data encoding known as Protobuf.

.. code:: python

   # Load GTFS-Realtime feeds.
   # For this example we will use publicly archived MTA data.
   import requests
   response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
   response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
   response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')

   stream = [response1.content, response2.content, response3.content]

We now have the raw bytes for a sequence of GTFS-Realtime feed updates.
Each update represents the state of the same wired-up slice of a transit
network at a different but consecutive point in time.

This is where ``gtfs_tripify`` comes in:

.. code:: python

   import gtfs_tripify as gt
   logbook, timestamps, parse_errors = gt.logify(stream)

Now we have a ``logbook``. If we inspect it we see that it is a ``dict``
with the following format:

.. code:: python

   {
       '87a19e7a-66dd-11e9-b1fe-8c8590adc94b': <pandas.DataFrame object>,
       '87a19db4-66dd-11e9-a0e0-8c8590adc94b': <pandas.DataFrame object>,
       ...
   }

Each entry in the ``logbook`` is a ``log``. Each log provides
information about a single train trip in the system.

.. code:: python

   print(logbook['87a19e7a-66dd-11e9-b1fe-8c8590adc94b'])

This looks something like this:

.. code:: python

    trip_id,route_id,action,minimum_time,maximum_time,stop_id,latest_information_time,unique_trip_id
    131750_7..N,7,STOPPED_OR_SKIPPED,1559440299.0,1559440695.0,726N,1559440315,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559440846.0,1559440860.0,725N,1559440860,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559440936.0,1559440950.0,724N,1559440950,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441016.0,1559441030.0,723N,1559441030,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441211.0,1559441226.0,721N,1559441226,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441291.0,1559441306.0,720N,1559441306,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441411.0,1559441426.0,719N,1559441426,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441561.0,1559441591.0,718N,1559441591,3ac1c948-af61-11e9-909a-8c8590adc94b
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441942.0,1559441956.0,712N,1559441956,3ac1c948-af61-11e9-909a-8c8590adc94b
