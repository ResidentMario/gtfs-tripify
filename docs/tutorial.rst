Tutorial
========

Interested in New York City transit? Want to learn more reasons why your particular train commute is good or bad? **This tutorial will show you how to roll your own daily MTA train arrival dataset using Python.** The result can then be used to explore questions about train service that schedule data alone couldn’t answer.

This tutorial assumes you've already read the `Quickstart`_.

.. _Quickstart: https://residentmario.github.io/gtfs-tripify/quickstart.html

Building a daily roll-up
------------------------

To begin, visit the MTA GTFS-RT Archive at http://web.mta.info/developers/data/archives.html:

|image0|

This page contains monthly rollups of realtime train location data in what is known as the “GTFS-RT format”. This is the data that powers both the train tracker apps on your phone and the arrival clocks on the station platforms, and the MTA helpful provides a historical archive of this data online.

The archive covers all train lines in the system. Pick a month that you are interested in, and click on the link to download it to your computer. Be prepared to wait a while; the files are roughly 30 GB in size.

Once the download is finished, you will have a file named something like ``201908.zip`` on your computer:

|image1|

Double click on this file to extract the files inside, and you will find that inside this ``zip`` file is *another* layer of ``zip`` files:

|image2|

Pick a day that you are interested in and double click on it again to extract the files. This will result in a folder containing many, many tiny files:

|image3|

Each of these sub-sub-files is a single GTFS-RT message. Each message is a snapshot of the state of a slice of the MTA system. It has important two properties:

-  The trains that this message covers.
-  The timestamp that this message represents information about.

For example, the file consider the file ``gtfs_7_20190601_042000.gtfs``. This file contains a snapshot of the state of all 7 trains in the MTA system as of 4:20 AM, June 1st, 2019.

Trains which run similar service routes may get “packaged up” into the same message. For example, the file ``gtfs_ace_20190601_075709.gtfs`` contains a snapshot of the state of all A, C, and E trains in the MTA system.

Some trains are packaged with out train lines, but seemingly for historical reasons are excluded from the name of the file:

-  The ``Z`` train is included in the ``gtfs_J`` messages.
-  The ``7X`` train is included in the ``gtfs_7`` messages.
-  The ``FS`` (Franklin Avenue Shuttle) and ``H`` (Rockaway Shuttle) are included in the ``ACE`` messages.
-  The ``W`` is included in the ``gtfs_NQR`` messages.

At this time, the following trains are *excluded* from the dataset, for unknown reasons:

-  The ``1``, ``2``, ``3``, ``4``, ``5``, ``6``, and ``6X`` trains do not appear in recent archives, although they appear to appear to have  been included in the archives in the past (`tracking issue`_).
-  The `late-night shuttles <https://en.wikipedia.org/wiki/S_(New_York_City_Subway_service)>`_.

.. _tracking issue: https://groups.google.com/forum/#!topic/mtadeveloperresources/pX3at6TWwY8

.. |image0| image:: https://i.imgur.com/Inma37H.png
.. |image1| image:: https://i.imgur.com/Reb47hY.png
.. |image2| image:: https://i.imgur.com/hPipcpY.png
.. |image3| image:: https://i.imgur.com/fGPAKQT.png

Now that we understand how to get the trains we want, let’s talk about timestamps. The MTA system updates several times per minute; the exact interval and the reliability of the update sequence varies. Each of these updates is timestamped in EST.

So for example, the ``gtfs_7_20190601_042000.gtfs`` message we talked about earlier represents a snapshot dating from 4:20 AM sharp on January 1st 2019. The message that immediately follows, ``gtfs_7_20190601_042015.gtfs``, is a snapshot of the system as of 4:20:15 on January 1st 2019, e.g. 15 seconds later; and so on.

Choose a train line or set of train lines, and copy the subset of the files whose arrival times you are interested in. For the purposes of this demo, I will grab data on every 7 train that ran on January 1st 2019. Paste this into another folder somewhere on your computer.

This data is snapshot data in an encoded binary format known as a `Protocol buffer`_. We now need to convert it into tabular data that we can actually analyze. This is actually an extremely tricky and difficult process. Luckily we can just use ``gtfs_tripify`` to handle this part of the process. To begin, install ``gtfs_tripify`` using ``pip`` from the command line:

.. code:: bash

   pip install gtfs_tripify

Navigate to that folder you dumped the files you are interested in, and execute the following command line instruction:

.. code:: bash

    gtfs_tripify logify ./ stops.csv --to csv --clean

This script may take a few tens of minutes to finish running. While processing the feeds, you will likely see many non-fatal warnings about data errors and printed to your terminal. These are dealt with automatically, and are safe to ignore for now; refer to the section `parse errors`_ for a reference on what they mean.

.. _parse errors: https://residentmario.github.io/gtfs-tripify/parse_errors.html

There is one small but important difference between this script execution and the one in the quickstart: the presence of the ``--clean`` flag. Setting this flag does two things.

First, it removes incomplete trips from the logbook. Incomplete trips are trips that started before the first feed message or ended after the last feed message. We don't have enough data to tell when or where these started or ended&mdash;they are incomplete.

Second, it removes trip cancellation stubs from the logbook. Trip cancellation stubs are artifact stops left over when the trip ID of a train is changed mid-route. It's impossible to know for sure when this occurs in all cases, due to the snapshot nature of the underlying data stream. ``gtfs_tripify`` uses a best-effort heuristic which is ~98% effective at detecting and removing these non-stops, **but may lose data near the last stop of the trip if the distance between updates is unusually long**.

This creates the practical constraint that ``gtfs_tripify`` is only as reliable as the underlying feed. Feed downtimes which are more than a few minutes in length causes the quality of the data produced by ``gtfs_tripify`` to start to degrade.

Successfully completely processing will write a fresh ``stops.csv`` file to your machine with an easy-to-read tabular rollup of your data:

.. _Protocol buffer: https://developers.google.com/protocol-buffers/

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
   131750_7..N,7,STOPPED_OR_SKIPPED,1559441942.0,1559441956.0,712N,1559441956,3ac1c948-af61-11e9-909a-8c8590adc94

At this point you can jump into your favorite data analysis environment and start exploring!

Building a larger dataset
-------------------------

How big a dataset can you build? ``gtfs_tripify`` does all of its processing in-memory, so it can only consume as many messages as will fit in your computer’s RAM at once. On my (16 GB) machine for example, I can only process data one day at a time.

To work around this limitation, build your datasets one time period at a time, then merge them together using the ``merge`` command. For example, suppose we've already built two logbooks with ``logify``, one for 7 trains that ran on July 1 2019 (``7_1_2019_7_stops.csv``) and one for 7 trains that ran on July 2 2019 (``7_2_2019_7_stops.csv``).

Note that these must be "dirty" logbooks, e.g. ones run with the ``--no-clean`` flag; we will handle discarding trips that fall outside of the combined time period in the merge step.

Now run the following command:

.. code:: bash

    gtfs_tripify merge 7_1_2019_7_stops.csv 7_2_2019_7_stops.csv stops.csv --to csv --clean

Alternatively, you can run the following Python script (or modify it to your purposes), which does the same thing:

.. code:: python

   import gtfs_tripify as gt
   from zipfile import ZipFile
   import os

   # Update this value with the path to the GTFS-RT rollup on your local machine.
   DOWNLOAD_URL = '~/Downloads/201906.zip'

   z = ZipFile(DOWNLOAD_URL)
   z.extract('20190601.zip')
   z.extract('201906012.zip')

   messages = []
   # filter out non-GTFS files
   for filename in sorted(os.listdir('.')):
       if '.py' not in filename and 'gtfs_7_' in filename:
           with open(filename, 'rb') as f:
               messages.append(f.read())

   # build the logbooks
   first_logbook, first_logbook_timestamps, _ = gt.logify(messages[:len(messages) // 2])
   second_logbook, second_logbook_timestamps, _ = gt.logify(messages[len(messages) // 2:])

   # merge the logbooks
   logbook = gt.ops.merge_logbooks(
       [(first_logbook, first_logbook_timestamps), (second_logbook, second_logbook_timestamps)],
       'logbook.csv'
   )

   # save to disk
   gt.ops.to_csv(logbook, 'logbook.csv')


To learn more, see the section `Additional methods`_.

.. _Additional methods: https://residentmario.github.io/gtfs-tripify/additional_methods.html
.. _Unix timestamp: https://en.wikipedia.org/wiki/Unix_time

Conclusion
----------

That concludes this tutorial. The next section, `Data analysis demo`_, showcases this data in
action.

.. _Data analysis demo: https://residentmario.github.io/gtfs-tripify/data_analysis_demo.html
