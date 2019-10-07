Quickstart
==========

In this section we'll build a small sample train arrivals dataset using
the ``gtfs_tripify`` command-line tool.

Begin by running the following to install this package on your local
machine:

.. code:: sh

   pip install gtfs_tripify

First we need to prepare our GTFS-Realtime feeds of interest.
GTFS-Realtime is a highly compressed binary format encoded using a
Google data encoding known as Protobuf. For the purposes of this demo,
we'll use some example data from the MTA archive.

.. code:: bash

    mkdir messages; cd messages
    curl -O https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31
    curl -O https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36
    curl -O https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41

Each of these updates is a snapshot of a piece of the MTA subway system at
a certain timestamp. By analyzing these snapshots for differences over time,
it is possible to reconstruct all of the stops these trains made.

.. code:: bash

    gtfs_tripify logify gtfs_messages/ stops.csv --to csv --no-clean

This outputs a ``logbook`` of individal trip ``logs``. This looks something
like this:

.. code:: python

    trip_id,route_id,action,minimum_time,maximum_time,stop_id,latest_information_time,unique_trip_id
    131750_7..N,7,STOPPED_OR_SKIPPED,1559440299.0,1559440695.0,726N,1559440315
    131750_7..N,7,STOPPED_OR_SKIPPED,1559440846.0,1559440860.0,725N,1559440860
    131750_7..N,7,STOPPED_OR_SKIPPED,1559440936.0,1559440950.0,724N,1559440950
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441016.0,1559441030.0,723N,1559441030
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441211.0,1559441226.0,721N,1559441226
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441291.0,1559441306.0,720N,1559441306
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441411.0,1559441426.0,719N,1559441426
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441561.0,1559441591.0,718N,1559441591
    131750_7..N,7,STOPPED_OR_SKIPPED,1559441942.0,1559441956.0,712N,1559441956

This dataset has the following schema:

-  ``trip_id``: The ID assigned to the trip in the GTFS-Realtime record. 
-  ``route_id``: The ID of the route. In New York City these are easy to read: 2 means this is a number 2 train.
-  ``stop_id``: The ID assigned to the stop in question.
-  ``action``: The action that the given train took at the given stop. One of ``STOPPED_AT``, ``STOPPED_OR_SKIPPED``, or ``EN_ROUTE_TO`` (the latter only occurs if the trip was still in progress as of the last message in the feed list).
-  ``minimum_time``: The minimum time at which the train pulled into the station. May be ``NaN``. This time is a `Unix timestamp`_.
-  ``maximum_time``: The maximum time at which the train pulled out of the station. May be ``NaN``. Also a Unix timestamp.
-  ``latest_information_time``: The timestamp of the most recent GTFS-Realtime data feed containing information pertinent to this record. Also a Unix timestamp.

.. _Unix timestamp: https://en.wikipedia.org/wiki/Unix_time

That concludes this brisk introduction to the ``gtfs_tripify`` library. For a more detailed
demonstration of ``gtfs_tripify`` features using the Python library, see see the `Tutorial`_
section. For a demonstratory analysis of one day's worth of data, see the section
`Data analysis demo`_.

.. _Tutorial: https://residentmario.github.io/gtfs-tripify/tutorial.html
.. _Data analysis demo: https://residentmario.github.io/gtfs-tripify/data_analysis_demo.html
