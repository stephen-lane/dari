**********************
Database Configuration
**********************

All database specific configuration parameters are prefixed with
**``dari/database/{databaseName}/``**.

**Key:** ``dari/defaultDatabase`` **Type:** ``java.lang.String``

    The name of the default database.

**Key:** ``dari/database/{databaseName}/class`` **Type:**
``java.lang.String``

    The classname of a ``com.psddev.dari.db.Database`` implementation.

**Key:** ``dari/database/{databaseName}/readTimeout`` **Type:**
``java.lang.Double``

    Sets the read timeout for this database. *The default is 3 seconds.*

**Key:** ``dari/databaseWriteRetryLimit`` **Type:**
``java.lang.Integer``

    The number of times to retry a transient failure. *The default value
    is 10.*

**Key:** ``dari/databaseWriteRetryInitialPause`` **Type:**
``java.lang.Integer``

    The initial amount of time in milliseconds to wait before retrying a
    transient failure. *The default value is 10ms.*

**Key:** ``dari/databaseWriteRetryFinalPause`` **Type:**
``java.lang.Integer``

    The maximum amount of time in milliseconds to wait before retrying a
    transient failure. *The default value is 1000ms.*

**Key:** ``dari/databaseWriteRetryPauseJitter`` **Type:**
``java.lang.Double``

    The amount of time to adjust the pause between retries so that
    multiple threads retrying at the same time will stagger. This helps
    break deadlocks in certain databases like MySQL. *The default value
    is 0.5.*

    The pause value is calculated as
    ``initialPause + (finalPause - initialPause) > * i / (limit - 1)``.
    This is then jittered + or - ``pauseJitter`` percent.

    For example, if ``dari/databaseWriteRetryLimit`` is 10,
    ``dari/databaseWriteRetryFinalPause`` is 1000ms and
    ``dari/databaseWriteRetryPauseJitter`` is 0.5 then on the first
    retry Dari will wait between 5ms and 15ms. On the second try Dari
    will wait between 60ms and 180ms continuing until 10th and final try
    which will wait between 500ms and 1500ms.

SQL Database Configuration
==========================

**Key:** ``dari/database/{name}/class`` **Type:** ``java.lang.String``

    This should be ``com.psddev.dari.db.SqlDatabase`` for all SQL
    databases.

**Key:** ``dari/isCompressSqlData`` **Type:** ``java.lang.Boolean``

    Enable or disable compression of Dari object data in the database.
    Dari uses the Snappy compression library for compression. To use
    this you must include Snappy in your pom.xml file as follows:

     org.iq80.snappy snappy 0.2

    *The default is false.* We recommend only enabling compression if
    you know your dataset is large (over 50GB).

**Key:** ``dari/database/{databaseName}/jdbcUrl`` **Type:**
``java.lang.String``

**Key:** ``dari/database/{databaseName}/readJdbcUrl`` **Type:**
``java.lang.String`` *(Optional)*

    The database jdbc url. All writes will go the database configured by
    ``jdbcUrl``. To have reads to go to a slave configure
    ``readJbdcUrl``.

**Key:** ``dari/database/{databaseName}/jdbcUser`` **Type:**
``java.lang.String``

**Key:** ``dari/database/{databaseName}/readJdbcUser`` **Type:**
``java.lang.String`` *(Optional)*

    The database user name.

**Key:** ``dari/database/{databaseName}/jdbcPassword`` **Type:**
``java.lang.String``

**Key:** ``dari/database/{databaseName}/readJdbcPassword`` **Type:**
``java.lang.String`` *(Optional)*

    The database password.

**Key:** ``dari/database/{databaseName}/dataSource`` **Type:**
``Resource``

**Key:** ``dari/database/{databaseName}/readDataSource`` **Type:**
``Resource`` *(Optional)*

    The database resource. All writes will go the database configured by
    ``dataSource``. To have reads to go to a slave configure
    ``readDataSource``.

.. note::
       To use Tomcat connection pooling define a JNDI Resource in
       ```<code>context.xml</code>``` with the name
       ```<code>dari/database/{databaseName}/dataSource</code>```

Aggregate Database Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Aggregate database is an implemention of
``com.psddev.dari.db.AbstractDatabase`` provided by Dari that allows
objects to be written to and read from multiple database backends.
Typically this is used to reads and writes to both MySQL and Solr. This
allows normal reads to go to MySQL, while full-text search will use
Solr.

**Key:** ``dari/database/{databaseName}/defaultDelegate`` **Type:**
``java.lang.String``

    This is the name of the primary database. It will be written to
    first and should be considered the source of record for all objects.
    This is usually one of the SQL backends.

Example Configuration
^^^^^^^^^^^^^^^^^^^^^

This is an example configuration that reads from a MySQL slave and
writes to a MySQL master. Solr is configured to read and write to the
same host.

::

    # Aggregate Database Configuration
    dari/defaultDatabase = production
    dari/database/production/defaultDelegate = sql
    dari/database/production/class = com.psddev.dari.db.AggregateDatabase
    dari/database/production/delegate/sql/class = com.psddev.dari.db.SqlDatabase

    # Master Configuration
    dari/database/production/delegate/sql/jdbcUser = username
    dari/database/production/delegate/sql/jdbcPassword = password
    dari/database/production/delegate/sql/jdbcUrl = jdbc:msyql://master.mycompany.com:3306/dari

    # Slave Configuration
    dari/database/production/delegate/sql/readJdbcUser = username
    dari/database/production/delegate/sql/readJdbcPassword = password
    dari/database/production/delegate/sql/readJdbcUrl = jdbc:msyql://slave.mycompany.com:3306/dari

    # Solr Configuration
    dari/database/production/delegate/solr/class = com.psddev.dari.db.SolrDatabase
    dari/database/production/delegate/solr/serverUrl = http://solr.mycompany.com/solr

Solr Database Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Key:** ``dari/database/{databaseName}/class`` **Type:**
``java.lang.String``

    This should be ``com.psddev.dar.db.SolrDatabase`` for Solr
    databases.

**Key:** ``dari/database/{databaseName}/serverUrl`` **Type:**
``java.lang.String``

    The URL to the master Solr server.

**Key:** ``dari/database/{databaseName}/readServerUrl`` **Type:**
``java.lang.String`` *(Optional)*

    The URL to slave Solr server.

**Key:** ``dari/database/{databaseName}/commitWithin`` **Type:**
``java.lang.Integer``

    The maximum amount of time in seconds to wait before committing to
    Solr.

**Key:** ``dari/database/{databaseName}/saveData`` **Type:**
``java.lang.Boolean``

    Disable saving of Dari record data (JSON Blob) to Solr. Disabling
    this will reduce the size of the Solr index at the cost of extra
    reads to the MySQL database. Only enable this if you have another
    database configured as the primary.

**Key:** ``dari/subQueryResolveLimit`` **Type:** ``java.lang.Integer``

    Since Solr does not currently support joins Dari will execute
    subqueries separately. This limits the size of the results used to
    prevent generating too large of a query.