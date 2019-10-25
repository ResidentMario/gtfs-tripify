.. gtfs_tripify documentation master file, created by
   sphinx-quickstart on Fri Jul 26 13:58:10 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

gtfs_tripify
============

Most major transit municipalities in the United States publish realtime train (and bus) arrival information using a specification known as `GTFS-RT <https://developers.google.com/transit/gtfs-realtime/>`_. ``gtfs_tripify`` is a Python package for turning an archived stream of GTFS-RT messages into a historical record of train (or bus) arrival and departure times—information that was previously only available in schedule form.

.. toctree::
   :maxdepth: 1

   installation.rst
   quickstart.rst
   tutorial.rst
   data_analysis_demo.rst
   parse_errors.rst
   api_reference.rst
   further_reading.rst
