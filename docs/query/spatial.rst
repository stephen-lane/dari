***************
Spatial Queries
***************

Dari supports spatial queries on MySQL, PostgreSQL and Solr. To use Dari's spatial features define a field of type ``com.psddev.dari.db.Location`` on the model you want to do spatial lookups on. This type is a container for latitude and longitude values. This field should be annotated with the ``@Index`` annotation.

For example:

.. code-block:: java

    public class Venue {
        private String name;
        @Index private Location location;

        // Getters and Setters
    }

To find all venues within a 10 mile radius of Reston Town Center in
Reston, VA we would issue the following query:

.. code-block:: java

    PaginatedResult<Venue> venues = Query.from(Venue.class).
        where("location = ?", Region.sphericalCircle(38.95854, -77.35815, 10));

Sorting venues by closest works as well:

.. code-block:: java

    PaginatedResult<Venue> venues = Query.from(Venue.class).
        where("location = ?", Region.sphericalCircle(38.95854, -77.35815, 10)).
        sortClosest("location", new Location(38.95854, -77.35815));

.. note::

    When using ```sortClosest``` you should limit the results to be inside a given distance with a ```WHERE``` clause. This will speed up your query.