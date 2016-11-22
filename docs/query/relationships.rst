**********************
Querying Relationships
**********************

Dari supports querying relationships using path notation (i.e. field/subfield)
in `WHERE` clauses. A common use case is finding all articles by a particular
author. We'll use the following models to demonstrate how to use path notation.

::

    public class Article extends Record {
        @Index private Author author;
        private String title;
        private String body;

        // Getters and Setters...
    }

    public class Author extends Record {
        private String firstName;
        @Index private String lastName;
        @Index private String email;

        // Getters and Setters...
    }

There are two ways we can find articles by a specific author. We can
query for the author first and then query for articles by that
author.

::

    Author author = Query.from(Author.class).where("email = 'john.smith@psddev.com'");
    List<Articles> = Query.from(Articles.class).where("author = ?", author);

However, it's easier and more efficient to do this in a single query
using path notation.

::

    List<Articles> = Query.from(Articles.class).where("author/email = 'john.smith@psddev.com'");