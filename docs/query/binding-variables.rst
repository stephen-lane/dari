**************
Bind variables
**************

In the previous section we used **?** in our **WHERE** clause when specifying the author. Dari supports bind
variables in query strings using **?** for placeholders.

.. code-block:: java

    String authorName = "John Smith";
    Author author = Query.from(Author.class).
                    where("name = ?", authorName).first();

Placeholders can be basic types like ``String`` or ``Integer`` but they can also be
Lists or other Dari objects. This allows for **IN** style queries.

.. code-block:: java

    List<String> names = new ArrayList<String>();
    names.add("John Smith");
    names.add("Jane Doe");
    List<Author> authors = Query.from(Author.class).
                            where("name = ?", names).selectAll();