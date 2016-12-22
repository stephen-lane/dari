***********************
The **ORDER BY** clause
***********************

Results can be ordered using ``sortAscending(String field)`` and ``sortDescending(String field)`` methods.  Both of these methods take the name of the field to sort. The field being sorted must have the ``@Indexed`` annotation.

.. code-block:: java

    List<Author> authors = Query.from(Author.class).sortAscending("name");