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

You can construct a larger logbook out of a contiguous sequence of smaller ones using ``merge_logbooks``:

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

As discussed in the `tutorial`_, this is a necessity when working with slices of time data too large to fit into memory. Note that ``merge`` has less robust logic for infering and merging trips than ``logify``, so to avoid trip fragmentation, use this method sparingly.

Removing incomplete trips
=========================

Parsing GTFS-RT messages naturally means specifying a starting point and and ending point for a period of time of interest. Trips that occur in the first message in a stream were already in progress at the beginning of that time window, and so you will only get partial information about these. Trips that appear in the last message in a stream are still in progress at the end of that time window, so again only partial information is available.

``logify`` will return these partial logs by default. If you want to include only complete trips in your dataset, run ``discard_partial_logs`` on your logbook to remove them:

.. code:: python

   pruned_logbook = gt.ops.discard_partial_logs(logbook)

Cutting cancelled stops
=======================

The `Demo data analysis`_ sections of the documentation discusses the **trip fragmentation** problem: trips that, due to the way the provider codes their systems, are split across multiple logs. This results in a large number of sequential ``STOPPED_OR_SKIPPED`` records in the output data for stops that never actually occurred, or were recorded in a different log in the logbook.

.. _Tutorial: https://residentmario.github.io/gtfs-tripify/tutorial.html
.. _Demo data analysis: https://residentmario.github.io/gtfs-tripify/demo_data_analysis.html

Removing these phantom stops is a necessity before you can save or analyze the dataset. ``cut_cancellations`` does this for you:

.. code:: python

   pruned_logbook = gt.ops.cut_cancellations(logbook)

``cut_cancellations`` is a best-attempt heuristic and may cause data loss when a train line is unusually short (e.g. a shuttle train with just two stops may complete its entire trip in the interval between snapshots) or the time delta between snapshots is unusually large (e.g. the feed went down). To prevent data loss, avoid running ``cut_cancellations`` on short-haul shuttle trains, and treat times when the feed was down with suspicion.

Partitioning a logbook on incompletes
=====================================

A log is complete if every message corresponding to that unique trip is in the stream. A log is incomplete if not&mdash;e.g. if the trip shows up in the first or last message in a stream, indicating that there are probably more messages before or after our time window that we don't have access to.

The "Removing incomplete trips" section discusses ``discard_partial_logs``, which can be used to purge such trips from the record. An alternative approach is to partition the logbook into two pieces: one logbook with every complete trip in the original logbook, and one logbook with every incomplete trip in the original logbook. ``partition_on_incomplete`` method does this for you:

.. code:: python

   complete_logbook, complete_timestamps, incomplete_logbook, incomplete_timestamps =\
       gt.ops.partition_on_incomplete(logbook, timestamps)

Partitioning a logbook on route
===============================

Another common task is partitioning a logbook on the ``route_id``, so that you can study each train route in isolation. This can be done using ``partition_on_route``:

.. code:: python

   logbooks_by_route, timestamps_by_route = gt.ops.partition_on_route(logbook, timestamps)

Saving a logbook to disk
========================

``to_csv`` will write a logbook to disk as a CSV file. You can use ``pandas.read_csv`` to read it back into memory as a CSV file, or ``from_csv`` to read it back into memory as a dictionary-keyed logbook.

.. code:: python

    gt.ops.to_csv(logbook, "trains.csv")
    logbook = gt.ops.from_csv("trains.csv")

Alternatively, ``to_gtfs`` can write a logbook to a `GTFS <https://developers.google.com/transit/gtfs/>`_ ``stop.txt`` record. This allows more direct comparison against scheduled GTFS data, but this should only be done on logbooks you've already executed ``gt.ops.cut_cancellations`` and ``gt.ops.discard_partial_logs`` on, as null values and projected stops are not supported by the GTFS schema.
