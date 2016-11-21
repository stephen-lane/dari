# Dari Reference Documentation

## Introduction

Dari is a simple, lightweight, Java library that automates many of the common development tasks involved with data storage and manipulation.

## Configuration

<a name="debug-servlet"></a>
### Debug Servlet

Dari provides a useful debug filter for use with servlet containers. The debug filter provides a simple query tool for
inspecting your Dari storage backend as well as some performance measurement and environment information tools.

To use the Debug servlet add the following to your `web.xml`:

    <filter>
        <filter-name>DebugFilter</filter-name>
        <filter-class>com.psddev.dari.util.DebugFilter</filter-class>
    </filter>
    <filter-mapping>
        <filter-name>DebugFilter</filter-name>
        <url-pattern>/*</url-pattern>
    </filter-mapping>

### Database Setup

- Setting up MySQL in both tomcat and command-line.
- Support for BoneCP.
- Support for Tomcat pooling.
- Setting up Solr.

### Storage Item Setup

- Setting up storage items.

## Modeling

Dari provides an object persistence API that allows Plain Old Java Objects to be persisted. It differs from traditional ORM implementations such as Hibernate in that it doesn't map classes to the underlying storage mechanism such as predefined tables. Instead it translates the object into a schema-less format appropriate for the chosen storage backend.

All objects to be persisted using Dari should subclass `com.psddev.dari.db.Record`. This class provides the basics necessary
to support persisting objects such as internal state information and methods for saving and deleting objects.

See also the <a href="#dari-query-language">Dari Query Language</a> section for information on how to query objects from the underlying storage.

<a name="object-example"></a>
### Example 

This example defines `Author`, `Tag` and `Article` objects with relationships. Classes can be modeled exactly how you normally would using plain Java. Dari will automatically handle the `Author` and `Tag` relationships in this example.

    public class Author extends Record {
        @FieldIndexed
        private private name;

        // Implementation of getters and setters.
    }

    public class Tag extends Record {
        @FieldIndexed
        private String name;

        // Implementation of getters and setters.
    }

    public class Article extends Record {
        private String title;
        private String body;
        
        @FieldIndexed
        private Author author;
        private List<Tag> tags;
    
        // Implementation of getters and setters.
    }

This is all that's required to persist an object.  Since these objects extend `Record` they will have `save()` method that can be used to persist them.

    Author author = new Author();
    author.setName("John Smith");
    author.save();

    Article article = new Article();
    article.setAuthor(author);
    article.setTitle("Today's Top News");
    article.setBody("<h1>Lots going on in the world.</h1>");
    article.save();

#### Relationships

Modeling relationships is as simple as adding field to your Java class. Dari will automatically keep track of the relationship. Referenced objects are lazy loaded when they are accessed for the first time.

#### Inheritance

Inheritance works just as you would expect in Dari, subclasses will have all the fields of their parent classes. 

It is possible to query for all objects of a given parent class and get instances of all subclasses.

    public class Event extends Record {
        @FieldIndexed
        private Date eventDate;
        private String eventDescription;

        // Implementation of getters and setters.

    }

    public class Checkin extends Event {    
        private String location;

        // Implementation of getters and setters.
    }

    public class Liked extends Event {       
        private String url;

        // Implementation of getters and setters.
    }

    public class Comment extends Event {
        private String comment;        

        // Implementation of getters and setters.
    }

Using the model above it is easy to query for all `Event` instances:

    List<Event> events = Query.from(Event.class).select();

It is possible to query for several specific types as well. For instance, to fetch all 
instances of `Liked` and `Comment` classes:

    List<RecordType> types = new ArrayList<RecordType>();
    types.add(ObjectType.getInstance(Liked.class));
    types.add(ObjectType.getInstance(Comment.class));

    List<Event> events = Query.from(Event.class).
                         where("typeId = ?", types).select();


#### Class Annotations

`@Embedded`

`@LabelFields`

`@DisplayName`

#### Field Annotations

`@FieldRequired` 

This annotation makes the field required. Attempting to save an object with a null value in a 
field marked `@FieldRequired` will result in an exception.

`@FieldIndex` 

This annotation enables indexing for a field. This is required by some storage backends to support
filtering and sorting.

`@FieldUnique` 

This annotation makes the field unique. This also implicitly adds `@FieldIndexed` to the field.

`@FieldEmbedded` 

This annotation makes the field embedded. This is intended to be used on fields that are 
Dari objects. It will embed their contents inside the current object instead of using referencing.

`@FieldPattern`

`@FieldTypes`

This annotation limits the types of Dari objects that can be set to a field. This is intended to be
used with a field of the base Dari type, `Record`.

`@FieldValues`

This annotation limits the values of a field.

#### Indexing

Indexing individual columns of a model allows the model to be queried by the indexed columns. Each
storage backend handles indexing in a different way.

##### MySQL

The MySQL storage backend requires that individual columns be indexed before using those columns in a
_where_ clause or sorting.

If an index is added after data is already saved to the database then all instances of the object with
the new index will need to be re-indexed before it can be used. 

TODO: Once fix-indexes is moved into Dari proper document it here.

<a name="dari-query-language"></a>
## Dari Query Language 

Dari provides a database-abstraction API that lets you retrieve your <a href="#modeling">data models</a>. Queries
are represented by instances of the `Query` class. The `Query` class should look very familiar to anybody that has used SQL before.

This guide will use the example models found in the <a href="#object-example">Modeling</a> section of this
documentation.

### The from clause

The simplest Dari query is:

    List <Author> authors = Query.from(Author.class).select();

This will return all instances of the class `Author.class`.

### The where clause

The `where` method allows you to filter the instances that are returned. Depending on the storage backend
it may be required to index the field being filtered on. Currently the MySQL storage backend requires fields 
used in a query to be indexed using the `@FieldIndexed` annotation.

    Author author = Query.from(Author.class).where("name = 'John Smith'").first();

This will return the instance of `Author` with the name 'John Smith'.

Logical operations `not, or, and` are supported.

    List<Author> authors = Query.from(Author.class).
                           where("name = 'John Smith' or name = 'Jane Doe'").select();

The `Query` class follows the builder pattern so this query can also be written as:

    List<Author> authors = Query.from(Author.class).
                           where("name = 'John Smith'").
                           and("name = 'Jane Doe'").select();

### The order by clause

Results can be ordered using `sortAscending()` and `sortDescending()` methods. Both of these methods take 
the name of the field to sort.

    List<Author> authors = Query.from(Author.class).sortAscending("name");

### Aggregate Functions

- groupCount
- count

### Bind variables

`Query` supports bind variables in query strings.

    String authorName = "John Smith";
    Author author = Query.from(Author.class).
                    where("name = ?", authorName).first();

Using bind variables allows for `IN` style queries by passing a list.

    List<String> names = new ArrayList<String>();
    names.add("John Smith");
    names.add("Jane Doe");
    List<Author> authors = Query.from(Author.class).
                           where("name = ?", names).select();

### Result Sets

Up until this point all the query examples either returned the first result, `first()`, or
all results, `select()`. `Query` also supports return a subset of results.

    PaginatedResult<Article> articles = Query.from(Article.class).
                                         sortAscending("title").select(0, 10);

This will start at offset 0 and return the next 10 instances of `Article`. The result of
a limit query is a `PaginatedResult`. It provides methods such as `hasNext()` and `getNextOffset()` for 
building pagination.

## Utilities

### Storage System

The Storage System abstracts file storage in model classes. It makes it easy to use local
file storage during development and use cloud storage in production.

To use the Storage System in models include a field with the type `StorageItem`. `StorageItem` is
an interface of which there are currently two main implementations: `AmazonStorageItem` and `LocalStorageItem`.

`LocalStorageItem` stores files to local disk.

`AmazonStorageItem` stores files to Amazon S3.

The `StorageItem` interface has a few convenience methods to create a new StorageItem based on the current
configuration:

    StorageItem item = StorageItem.create();
    item.setData(is);
    item.save(); 

#### Configuration

Here is an example `context.xml` configuration that will use the `AmazonStorageItem` by default when create new storage items.

    <Environment name="dari/defaultStorage" override="false" type="java.lang.String" value="s3" />

    <!-- Configure LocalStorageItem  -->
    <Environment name="dari/storage/local/class" type="java.lang.String" value="com.psddev.dari.util.LocalStorageItem" override="false" />
    <Environment name="dari/storage/local/rootPath" type="java.lang.String" value="/mnt/storage" override="false" />
    <Environment name="dari/storage/local/baseUrl" type="java.lang.String" value="http://local.psddev.com/storage" override="false" />

    <!-- Configure AmazonStorageItem -->
    <Environment name="dari/storage/s3/class" type="java.lang.String" value="com.psddev.dari.util.AmazonStorageItem" override="false" />
    <Environment name="dari/storage/s3/access" type="java.lang.String" value="<access token>" override="false" />
    <Environment name="dari/storage/s3/secret" type="java.lang.String" value="<secret>" override="false" />
    <Environment name="dari/storage/s3/bucket" type="java.lang.String" value="<bucket>" override="false" />
    <Environment name="dari/storage/s3/baseUrl" type="java.lang.String" value="http://<hostname>.cloudfront.net" override="false" />

### Settings

### Caching

## Advanced

- Transactions.
- Tasks.
- Internal state.
- Object Modifications.
- MySQL query hints.

## Debugging

### Low-level Debugging

Some times it is useful to see exactly what Dari is sending to a storage backend. This can be used to 
debug a query that is performing slowly or one that isn't returning any data when you expect it to.

Most storage backends provide a method that will turn a `Query` instance into a string query that it will
use to send to the underlying storage.

    Query query = Query.from(Article.class).where("title = ?", "What's New");
    System.out.println(Database.Static.getFirst(SqlDatabase.class).buildSelectStatement(query));

See the <a href="#storage-backends">storage backend</a> documentation for more information on how each
storage backend stores and indexes it's data.

- Explain _log url query parameters

### Query Tool

### Status Tool

The status tool provides insight into the performance of Dari when running inside a servlet container. It 
provides four sets of information: _task execution time, running tasks, jsp execution time and storage 
engine execution time_.

The storage engine execution time is displayed along with the raw query that was sent to the storage backend. This
allows for quickly diagnosing performance issues. For example, you can copy and paste a MySQL backend raw query and 
run an EXPLAIN on it.

The status tool is automatically enabled as part of the <a href="#debug-servlet">Debug Servlet</a>. It can be found at the URL `/_debug/status`.

- _debug JSPs

<a name="storage-backends"></a>
## Storage Backends

Dari is designed in such a way that it can have multiple storage backends. It can, and usually does, use several
of them at once. For example, a standard configuration is to store data in MySQL as the source of record and also
store data in Solr for full text search capabilities.

Dari will automatically handle saving to all configured storage backends when the `save()` method is called on a
record.

### MySQL Storage Backend

The MySQL storage backend is the recommended storage backend to use as your source of record. The MySQL storage
backend has been tested with MySQL version 5.1.x and up. It requires Innodb and UTF-8 support.

#### Tables

This storage backend is implemented using six tables. The primary table, `Record`, stores the raw information about
the objects that are saved. Objects are serialized to JSON format when they are saved and that is stored in the `data`
in the `Record` table.

The rest of the tables are used to implement indexes. They are `RecordString`, `RecordLocation`, `RecordNumber`, 
`RecordUpdate` and `RecordUuid`.

The record ID and type ID fields are stored as a binary representation of a UUID. You can use the `hex()` method in MySQL to convert them to readable string when debugging directly with SQL queries in a MySQL client. When running raw queries in MySQL
prefixed the value from `hex()` with `0x`. If you already have a UUID for a record, prefix it with `0x` and remove
the dashes. For example to query MySQL directly for an object with the UUID of __550e8400-e29b-41d4-a716-446655440000__:

    SELECT hex(id), hex(typeId), data FROM Record WHERE id = 0x550e8400e29b41d4a716446655440000;

#### Indexing

Since records are stored as a JSON text blob in MySQL Dari cannot use traditional MySQL indexing. To implement
indexing Dari uses several additional tables to store indexes.

There are currently three primary index tables: `RecordString`, `RecordNumber` and `RecordUuid`.

When fields of an object are indexed the value along with the object it belongs to will be stored in the appropriate
index table.

##### Indexing Example

Here is a simple class that has a String field indexed.

    public class Article extends Record {
        @FieldIndexed
        private String title;
    }

When an instance of this class is saved it will insert a row in the `Record` table containing the raw object data. It
will also insert a row into the `RecordString` like the following:

     recordId                               | name                 | value
     -----------------------------------------------------------------------------------------
    | 550e8400-e29b-41d4-a716-446655440000  | Record/title         | What's New in Java 7     |
     -----------------------------------------------------------------------------------------

The `recordId` field references back to the record in the `Record` table that we're indexing.

`RecordNumber` is used to index number fields. `RecordUuid` is used to index object references.

### Solr Storage Backend

- All fields are automatically indexed.

