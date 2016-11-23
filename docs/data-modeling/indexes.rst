*******
Indexes
*******

In order to query an object by one of its fields the fields must be
marked with the ``@Indexed`` annotation. This lets Dari know that these
fields can be queried against. Without this annotation Dari will not
allow queries against a field.

.. note::

    Every field marked with an index will be an cause an additional row to be written to the underlying database index tables when using a SQL database backend.

Sometimes it is necessary to add indexes to existing models that already
have data. Just like a traditional SQL databases this requires the index
to be filled for all the existing data. In Dari there is a Bulk
Operations tool to help manage this at ``/_debug/db-bulk``. This tool
will re-index all instances of a selected model.

After starting a bulk operation it's progress can be monitored on the
Task Manager page (``/_debug/task``).

See the `Configuration`_ section of the documentation for information on
how to configure the debug tools.

|Bulk Operations Tool|

.. |Bulk Operations Tool| image:: images/bulk-operations.png

.. _Configuration: /dari/configuration/debug-tools.html