***********
Collections
***********

Collections (e.g List, Set, etc) are automatically handled by Dari. No
extra tables or information is needed.

For example we can add a ``Tag`` model and add a list of tags to the
``Article`` class from above:

.. code-block:: java

    import com.psddev.dari.db.Record;

    public class Tag extends Record { 
        private String name;

        // Getters and Setters

    }

.. code-block:: java

    import com.psddev.dari.db.Record; 
    import com.psddev.dari.db.ReferentialText;

    public class Article extends Record { 
    
        @Indexed private Author author;
        @Indexed private String title; 
        private ReferentialText body; 
        private List tags;

        // Getters and Setters

    }

.. raw:: html

.. note:: 

    Indexes on collections are also supported. However, performance should be
    considered when adding indexes to collections. Each item in a
    collection will result in an additional row to be written to the underlying database
    index tables when using a SQL database backend.

    Also be careful that your collections do not grow unbounded. Large
    collections can slow down retrieval of data since Dari will
    retrieve the collection data even if you don't need it.