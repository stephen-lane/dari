***********
Debug Tools
***********

To enable debug tools add the following to your web.xml:

::

    <filter>
        <filter-name>ApplicationFilter</filter-name>
        <filter-class>com.psddev.dari.db.ApplicationFilter</filter-class>
    </filter>
    <filter-mapping>
        <filter-name>ApplicationFilter</filter-name>
        <url-pattern>/*</url-pattern>
        <dispatcher>ERROR</dispatcher>
        <dispatcher>FORWARD</dispatcher>
        <dispatcher>INCLUDE</dispatcher>
        <dispatcher>REQUEST</dispatcher>
    </filter-mapping>

Debug Filter Configuration
==========================

**Key:** ``PRODUCTION`` **Type:** ``java.lang.Boolean``

    This key enables or disables *production* mode. When production mode
    is enabled a ``debugUsername`` and ``debugPassword`` are required to
    use any debug tools.

    This also suppresses JSP error messages in the browser. JSP errors
    will still show up in logs.

    This value defaults to false.

**Key:** ``dari/debugUsername`` **Type:** ``java.lang.String``

    The debug interface user name.

**Key:** ``dari/debugPassword`` **Type:** ``java.lang.String``

    The debug interface password.

**Key:** ``dari/debugRealm`` **Type:** ``java.lang.String``

    The debug interface realm.