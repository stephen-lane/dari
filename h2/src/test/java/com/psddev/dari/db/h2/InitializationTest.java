package com.psddev.dari.db.h2;

import com.psddev.dari.util.CollectionUtils;
import com.psddev.dari.util.SettingsException;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class InitializationTest {

    private static final String JDBC_URL = "jdbc:h2:mem:test" + UUID.randomUUID().toString().replaceAll("-", "") + ";DB_CLOSE_DELAY=-1";

    private H2Database database;
    private Map<String, Object> settings;

    @Before
    public void before() {
        database = new H2Database();
        settings = new HashMap<>();
    }

    @After
    public void after() {
        database.close();
    }

    private void put(String path, Object value) {
        CollectionUtils.putByPath(settings, path, value);
    }

    @Test
    public void dataSource() {
        HikariDataSource hikari = new HikariDataSource();
        hikari.setJdbcUrl(JDBC_URL);
        put(H2Database.DATA_SOURCE_SETTING, hikari);
        database.initialize("", settings);
    }

    @Test(expected = SettingsException.class)
    public void dataSourceNotDataSource() {
        put(H2Database.DATA_SOURCE_SETTING, "foo");
        database.initialize("", settings);
    }

    @Test
    public void driver() {
        put(H2Database.JDBC_URL_SETTING, JDBC_URL);
        put(H2Database.JDBC_DRIVER_CLASS_SETTING, org.h2.Driver.class.getName());
        database.initialize("", settings);
    }

    @Test(expected = SettingsException.class)
    public void driverNotFound() {
        put(H2Database.JDBC_URL_SETTING, JDBC_URL);
        put(H2Database.JDBC_DRIVER_CLASS_SETTING, "foo");
        database.initialize("", settings);
    }

    @Test(expected = SettingsException.class)
    public void driverNotDriver() {
        put(H2Database.JDBC_URL_SETTING, JDBC_URL);
        put(H2Database.JDBC_DRIVER_CLASS_SETTING, getClass().getName());
        database.initialize("", settings);
    }
}
