Additional methods
------------------

The ``ops`` submodue contains a variety of operations useful for working with logbooks.

``gtfs_tripify`` will by default provide as much information as possible, and will include both incomplete trips (trips which are still in progress as of the last message in the stream) and cancelled stops
(stops that did not occur due to trip cancellations). You may prune these:

.. code:: python

   import gtfs_tripify as gt
   import requests
   response1 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-31')
   response2 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-36')
   response3 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-41')
   stream = [response1.content, response2.content, response3.content]

   logbook, timestamps, parse_errors = gt.logify(stream)
   len(logbook)  # 313 logs included
   sum(len(log) for log in logbook)  # 11268 log entries included

   pruned_logbook = gt.ops.cut_cancellations(pruned_logbook)
   pruned_logbook = gt.ops.discard_partial_logs(pruned_logbook)

   len(pruned_logbook)  # 245 logs remaining
   sum(len(log) for log in pruned_logbook)  # 8820 log entries remaining

You may partition a logbook into complete and incomplete trip logbooks:

.. code:: python

   complete_logbook, complete_timestamps, incomplete_logbook, incomplete_timestamps =\
       gt.ops.partition_on_incomplete(logbook, timestamps)

Or partition a logbook based on route:

.. code:: python

   logbooks_by_route, timestamps_by_route = gt.ops.partition_on_route(logbook, timestamps)

You can construct a larger logbook out of a contiguous sequence of smaller ones:

.. code:: python

   old_logbook, old_timestamps = logbook, timestamps

   response4 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')
   response5 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-51')
   response6 = requests.get('https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46')

   new_stream = [response4.content, response5.content, response6.content]
   new_logbook, new_timestamps = gt.logify(new_stream)

   combined_logbook, combined_logbook_timestamps = gt.ops.merge(
       [(old_logbook, old_timestamps), (new_logbook, new_timestamps)]
   )

Note that ``gt.ops.merge`` does not have the robust logic for infering and merging trips that ``gt.logify`` has, so it’s highly recommended to only merge large logbooks to help avoid "trip fragmentation".

Finally, you may save a logbook to disk. There are a couple of methods for doing so: ``gt.ops.to_csv`` (and its companion ``gt.ops.from_csv``),
which will write a logbook to disk as a CSV file, and ``gt.ops.to_gtfs``, which will write a logbook to disk as a `GTFS <https://developers.google.com/transit/gtfs/>`_ ``stop.txt`` record. You should only use ``gt.ops.to_gfst`` on complete logbooks (e.g., ones which you have run ``gt.ops.cut_cancellations`` and ``gt.ops.discard_partial_logs`` on), as the GTFS spec allows neither null values nor hypothetical stops in ``stops.txt``, so the offending stop records will be ignored.
