Parse errors
============

The stream of updates you pass to ``gt.logify`` may contain any of a large number of non-fatal errors and data inconsistencies, a list of which is returned as part of the method’s output. This section documents what they are and how they are handled.

First, some terminology:

-  **update** — A single parsed GTFS-RT update.
-  **message** — An individual entity in an update. There are two kinds: trip update messages, which give schedule information, and vehicle update messages, which give train location information. Complimentary messages are linked by ``trip_id``.
-  **stream** — A sequential list of updates over time.

Now for the actual errors:

-  ``parsing_into_protobuf_raised_exception`` — Occurs when the bytes of a feed update cannot successfully be parsed into a Protobuf. This indicates data corruption. These messages are removed from the field. This will degrade the accuracy of the logbook estimates.
-  ``parsing_into_protobuf_raised_runtime_warning`` — Occurs when the bytes of a feed update can successfully be parsed into a Protobuf, but doing so raises a ``RuntimeWarning``. This likely indicates data loss, and since ``gtfs_tripify`` is sensitive to such data loss these messages are removed from the feed. This will degrade the accuracy of the logbook estimates.
-  ``message_with_null_trip_id`` — Occurs when a message in an update in the feed has its ``trip_id`` set to empty string (``''``). Empty strings are not valid trip identifiers and indicate an error by the feed provider. The offending messages are dropped.
-  ``trip_has_trip_update_with_no_stops_remaining`` — Occurs when there is a trip update (and optionally a complimentary vehicle update) which has no stops remaining. This is an error by the feed provider, as such trips are supposed to be removed from the feed upon arriving at their final stations. The messages corresponding with this ``trip_id`` are dropped.
-  ``trip_id_with_trip_update_but_no_vehicle_update`` — Occurs when there is a trip update with no complimentary vehicle update. This is an error by the feed provider: there is schedule information about a trip but no location information, which makes parsing that schedule impossible. The offending message is dropped.
-  ``feed_updates_with_duplicate_timestamps`` — Occurs when there are multiple updates in the feed with the same timestamp. This means that either a double read occurred or more likely the feed stopped updating and returned stale data. The offending updates are removed from the field.
-  ``feed_update_has_null_timestamp`` — Occurs when there is an update has its timestamp set to empty string (``''``) or ``0``. These values are null sentinels and indicate an error by the feed provider. The offending update is dropped.
-  ``feed_update_goes_backwards_in_time`` — Occurs when there is an update in the stream whose timestamp is a smaller value than that of the update immediately prior. This is an error by the feed provider as the stream cannot go backwards in time. The offending update is removed from the feed.

Each entry in ``parse_error`` includes the ``type`` of error, taken from the list above, as well as some additional ``details`` about the error helpful for debugging.
