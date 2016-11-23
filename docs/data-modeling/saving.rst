************************
Saving With Dari Objects
************************

To save a Dari object to the underlying database storage call the
`save()`_ method on the object.

.. code-block:: java

    Image image = new Image();
    image.setName("name");
    image.save();

    Article article = new Article();
    article.setArticle(article);
    article.setTitle("This is the Article Title");
    article.setBody("<h1>This is the Body Text</h1>");
    article.setImage(image)
    article.save();

To delete a Dari object from the underlying database storage call the
`delete()`_ method on the object.

.. _save(): https://github.com/perfectsense/dari/blob/master/db/src/main/java/com/psddev/dari/db/Record.java#L485
.. _delete(): https://github.com/perfectsense/dari/blob/master/db/src/main/java/com/psddev/dari/db/Record.java#L456