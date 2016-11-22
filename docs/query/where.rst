********************
The **WHERE** clause
********************

The `WHERE` method allows you to filter which object instances that are returned.
In order to filter by a field it must have the @Index annotation.

.. code-block:: java

    Author author = Query.from(Author.class).where("name = 'John Smith'").first();

This will return the first instance of `Author` with the name 'John Smith'.

Logical operations `not, or, and` are supported.

.. code-block:: java

    List<Author> authors = Query.from(Author.class).
                        where("name = 'John Smith' or name = 'Jane Doe'").selectAll();

The `Query` class follows the builder pattern so this query can also be written as:

.. code-block:: java

    List<Author> authors = Query.from(Author.class).
                        where("name = 'John Smith'").
                        and("name = 'Jane Doe'").selectAll();

For a list of all supported predicates see the `Predicate`_ documentation.

.. _Predicate: /dari/reference/predicates.html