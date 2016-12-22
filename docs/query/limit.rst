********************
The **LIMIT** clause
********************

Dari supports limiting the number of results returned using the ``select(long offset, int limit)`` or ``first()`` methods.

.. code-block:: java

    PaginatedResult<Article> articles = Query.from(Article.class).
                                            sortAscending("title").select(0, 10);
    List<Article> items = articles.getItems();

This will start at offset 0 and return the next 10 instances of `Article`. The result of a limit query is a ``PaginatedResult``. This class provides methods for working with paginated results as ``hasNext()`` and ``getNextOffset()`` for building pagination. To get all items in a PaginatedResult use
``getItems()``.