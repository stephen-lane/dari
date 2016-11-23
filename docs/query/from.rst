*******************
The **FROM** clause
*******************

The simplest Dari query is to select all records of a given type:

.. code-block:: java

    List<Author> authors = Query.from(Author.class).selectAll();

This will return all instances of the ``Author`` class.

Inheritance also works with the **FROM** clause by querying from a base
class. It is possible to build an activity feed, for example:

.. code-block:: java

    public class Activity extends Record {
        @Index private Date activityDate;
        @Index private User user;
    }

    public class Checkin extends Activity { ... }
    public class Comment extends Activity { ... }
    public class ReadArticle extends Activity { ... }
    public class PostedArticle extends Activity { ... }

Given this class heirarchy we can query for user's activity by querying
from the ``Activity`` class. This will implicitly also retrieve any
records that are subclasses of ``Activity``.

.. code-block:: java

    PaginatedResult<Activity> results = Query.from(Activity.class).
        where("user = ?", user).
        sortDescending("activityDate").select(0, 10);