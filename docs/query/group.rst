***********************
The **GROUP BY** clause
***********************

Using the ``groupBy(String... fields)`` method allows queries to return items in groupings, based on associations.  In the example below we will return a count of articles grouped by the tags associated with each.

To show how group works we'll use the following example Article that contains the Tag field that we will group by.

.. code-block:: java

    public class Article extends Record {
        private Tag tag;
        private String author;

        // Getters and Setters
    }

Now the groupBy query:

.. code-block:: java

    List<Grouping<Article>> groupings = Query.from(Article.class).groupBy("tag")

    for (Grouping grouping : groupings) {
        Tag tag = (Tag) grouping.getKeys().get(0);
        long count = grouping.getCount();
    }

It is possible to retrieve the items that make up a grouping by using the ``createItemsQuery()`` method on the returned ``Grouping`` objects. This method will return a ``Query`` object.

.. code-block:: java

    List<Grouping<Article>> groupings = Query.from(Article.class).groupBy("tag")

    for (Grouping grouping : groupings) {
        Tag tag = (Tag) grouping.getKeys().get(0);
        List<Article> articles = grouping.createItemsQuery().selectAll();
    }

Grouping by more than one item, for example, a Tag, and Author is
possible as well.

.. code-block:: java

    List<Grouping<Article>> groupings = Query.from(Article.class).groupBy("tag" , "author") 

    for (Grouping grouping : groupings) {
        Tag tag = (Tag) grouping.getKeys().get(0);
        Author author = (Author) grouping.getKeys().get(1);
        long count = grouping.getCount();
    }

To sort the count, add standard sorters;

.. code-block:: java

    List<Grouping<Article>> groupings = Query.from(Article.class).sortAscending("tag").groupBy("tag")

    for (Grouping grouping : groupings) {
        Tag tag = (Tag) grouping.getKeys().get(0);
        List<Article> articles = grouping.createItemsQuery().getSorters().clear().SelectAll();
    }