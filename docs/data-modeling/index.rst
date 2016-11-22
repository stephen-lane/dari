#############
Data Modeling
#############

All models in Dari extend from `com.psddev.dari.db.Record`_. Unlike
traditional ORMs Dari does not map objects to database tables. Instead
it serializes object data into JSON and stores it in the database as a
BLOB. This frees developers from worrying about creating or altering
database tables and allows for rapid evolution of data models.

All fields in a class that extends ``com.psddev.dari.db.Record`` are
persisted to the database when the object is saved. To prevent a field
from being persisted mark it as ``transient``.

Here is an example data model of an article with an author:

.. code-block:: java

    import com.psddev.dari.db.Record; 
    import com.psddev.dari.db.ReferentialText;

    public class Article extends Record { 
    
        @Indexed private Author author;
        @Indexed private String title; 
        private ReferentialText body;

        // Getters and Setters

    } 

.. code-block:: java

    import com.psddev.dari.db.Record;

    public class Author extends Record { 
        private String name; 
        @Indexed private String email;

        // Getters and Setters

    }

.. toctree:: 
    :hidden:
    
    indexes
    collections
    embedded
    relationships
    saving
    schema-viewer

.. _com.psddev.dari.db.Record: https://github.com/perfectsense/dari/blob/master/db/src/main/java/com/psddev/dari/db/Record.java