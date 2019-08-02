Additional methods
------------------

This section describes various utility methods from the ``ops`` submodule in ``gtfs_tripify``. The code samples in this section use the following ``logbook``:

.. code:: python

   import gtfs_tripify as gt
   import requests
   response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
   response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
   response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
   stream = [response1.content, response2.content, response3.content]

   logbook, timestamps, parse_errors = gt.logify(stream)


Merging logbooks
================

You can construct a larger logbook out of a contiguous sequence of smaller ones using ``gt.ops.merge_logbooks``:

.. code:: python

   import gtfs_tripify as gt
   import requests

   old_logbook, old_timestamps, old_parse_errors = logbook, timestamps, parse_errors

   response4 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')
   response5 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-51')
   response6 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')
   new_stream = [response4.content, response5.content, response6.content]
   new_logbook, new_timestamps, new_parse_errors = gt.logify(new_stream)

   combined_logbook, combined_logbook_timestamps = gt.ops.merge(
       [(old_logbook, old_timestamps), (new_logbook, new_timestamps)]
   )

As discussed in the `Tutorial`_ section, this is a necessity when working with slices of time data too large to fit into memory all at once. Note that ``gt.ops.merge`` does not as robust logic for infering and merging trips as ``gt.logify``, so to avoid trip fragmentation, use this method sparingly.

Removing incomplete trips
=========================

Parsing GTFS-RT messages naturally means specifying a starting point and and ending point for a perioud of time of interest. Trips that occur in the first message in a stream were already in progress at the beginning of that time window, and so you will only get partial information about these. Trips that appear in the last message in a stream are still in progress at the end of that time window, so again only partial information is available.

``gt.logify`` will return these partial logs by default. If you want to include only complete trips in your dataset, you can use ``gt.ops.discard_partial_logs`` to remove them:

.. code:: python

   pruned_logbook = gt.ops.discard_partial_logs(pruned_logbook)

Cutting cancelled stops
=======================

The `Tutorial`_ and `Demo data analysis`_ sections of the documentation discuss the problem of **trip fragmentation**: train trips that, due to the way the MTA codes their systems, are split across multiple logs. This results in a large number of sequential ``STOPPED_OR_SKIPPED`` records in the output data for stops that never actually occurred, or were recorded in a different log in the logbook.

.. _Tutorial: https://residentmario.github.io/gtfs-tripify/tutorial.html
.. _Demo data analysis: https://residentmario.github.io/gtfs-tripify/demo_data_analysis.html

Removing these phantom stops is a necessity before you can save or analyze the dataset. ``gt.ops.cut_cancellations`` is how you do this:

.. code:: python

   pruned_logbook = gt.ops.cut_cancellations(logbook)

Partitioning a logbook on incompletes
=====================================

A log is complete if every message corresponding to that unique trip is in the stream. A log is incomplete if not&mdash;e.g. if the trip shows up in the first or last message in a stream, indicating that there are probably more messages before or after our time window that we don't have access to.

The "Removing incomplete trips" section discusses ``gt.ops.discard_partial_logs``, which can be used to purge such trips from the record. An alternative approach is to partition the logbook into two pieces: one logbook with every complete trip in the original logbook, and one logbook with every incomplete trip in the original logbook. The ``gt.ops.partition_on_incomplete`` method does this for you:

.. code:: python

   complete_logbook, complete_timestamps, incomplete_logbook, incomplete_timestamps =\
       gt.ops.partition_on_incomplete(logbook, timestamps)

Partitioning a logbook on route
===============================

Another common task is partitioning a logbook on the train ``route_id``, so that you can study each train route in isolation. This can be done using ``gt.ops.partition_on_route``:

.. code:: python

   logbooks_by_route, timestamps_by_route = gt.ops.partition_on_route(logbook, timestamps)

Saving a logbook to disk
========================

``gt.ops.to_csv`` will write a logbook to disk. You can use a regular CSV reader, e.g. the ``read_csv`` method in ``pandas``, to read the data back into memory as a sequence of rows. Alternatively, the ``gt.ops.from_csv`` lets you read the data back into memory as a ``dict``-based logbook.

.. code:: python

    gt.ops.to_csv(logbook, "trains.csv")
    logbook = gt.ops.from_csv("trains.csv")

Alternatively, the ``gt.ops.to_gtfs`` method can be used to write a logbook to a `GTFS <https://developers.google.com/transit/gtfs/>`_ ``stop.txt`` record. This allows more direct comparison against scheduled GTFS data, but this method should only be run on complete logbooks (e.g., ones which you have already run ``gt.ops.cut_cancellations`` and ``gt.ops.discard_partial_logs`` on), as the GTFS spec does not allow null values or hypothetical stops in ``stops.txt``. For general use-cases, the ``to_csv`` method is preferable.
