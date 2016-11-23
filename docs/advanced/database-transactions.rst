************
Transactions
************

Transactions require obtaining an instance of the `Database` object you
want to run a transation on. Generally you want to use the default
database. Transactions are thread bound.

.. code-block:: java

    Database db = Database.Static.getDefault();
    db.beginWrites();
    try {
        // Save Dari objects using save().
        ...
        db.commitWrites();
    } finally {
        db.endWrites();
    }

Every call to ``beginWrites()`` must be followed by a call to ``endWrites()``.