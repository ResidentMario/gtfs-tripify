Quickstart
==========

``gtfs-tripify`` is a CLI and Python library for transforming archival GTFS-Realtime messages into
a tabular dataset of historical vehicle arrival and departure times. In this section of the
documentation, I will build a quick demonstration dataset using the ``gtfs-tripify`` command-line
interface.

To begin, make sure that you have Python 3.6 or newer `installed and active`_. Then run the
following ``pip`` package manager (comes included) command from your command line to install
``gtfs-tripify``:

.. _installed and active: https://realpython.com/installing-python/

.. code:: bash

   pip install gtfs_tripify

We will also need some data. For the purposes of this demo, we'll use some example data from the
MTA archive (this code snippet uses the ``curl`` Unix utility; Windows users, use a ``curl``
alternative or download these files by hand):

.. code:: bash

    curl -O https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31
    curl -O https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36
    curl -O https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41

This will create three GTFS-Realtime files on your machine, each containing a snapshot of the
state of the MTA system as of a certain date and time. We can turn these hard-to-read
binary-encoded messages into a simple CSV table using the ``gtfs_tripify`` CLI:

.. code:: bash

    gtfs_tripify logify ./ stops.csv --to csv --no-clean

This command tells ``gtfs_tripify`` to "logify" (transform into a tabular **trip log**) every
GTFS-Realtime message in the current folder and output the result to ``stops.csv``. ``--to csv``
instructs ``gtfs_tripify`` to output the data in a CSV format, and ``--no-clean`` instructs
``gtfs_tripify`` not to drop partial trips from the file (we are using just fifteen minutes of
data in this demo).

The resulting file looks something like this:

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
-  ``route_id``: The ID of the route (e.g. ``2`` train, ``3`` train, etcetera).
-  ``stop_id``: The ID assigned to the stop in question. To resolve this value to stop names,
   please see the GTFS file for this transit system. The MTA for example hosts this file at
   https://api.mta.info/.
-  ``action``: The action that the given train took at the given stop. One of ``STOPPED_AT``,
   ``STOPPED_OR_SKIPPED``, or ``EN_ROUTE_TO``.
-  ``minimum_time``: The minimum arrival time. `Unix timestamp`_. If the first snapshot included
   in the feed parse has this vehicle already at a station, this value will be set to null.
-  ``maximum_time``: The maximum departure time. If the trip cuts out without a vehicle having
   arrived at some of its stations this value will be set to null.
-  ``latest_information_time``: The timestamp of the most recent GTFS-Realtime data feed
   containing information pertinent to this record. Unix timestamp.

.. _Unix timestamp: https://en.wikipedia.org/wiki/Unix_time

That concludes this brisk introduction. For a more detailed demo see the `tutorial`_. To
get a better idea of what you can do with this data, see the `data analysis demo`_.

.. _tutorial: https://residentmario.github.io/gtfs-tripify/tutorial.html
.. _data analysis demo: https://residentmario.github.io/gtfs-tripify/data_analysis_demo.html
