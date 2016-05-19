package com.psddev.dari.util;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.collections.CollectionUtils;

import org.junit.*;
import static org.junit.Assert.*;

public class DateUtilsTest {
    private static final int SECOND = 1000;
    private static final int MINUTE = 60 * 1000;
    private static final int HOUR = 60 * 60 * 1000;
    private static final int DAY = 24 * 60 * 60 * 1000;

    @Before
    public void before() {}

    @After
    public void after() {}

    /**
     * public static Date fromString(String string, String format)
     */
    @Test 
    public void fromString_format_notz() throws ParseException {
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse("2011/05/13 12:45:27");
    	assertEquals(expect, DateUtils.fromString("2011/05/13 12:45:27", "yyyy/MM/dd HH:mm:ss"));
    }
    
    @Test 
    public void fromString_format_tz() throws ParseException {
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/12 EDT 12:45:27");
    	assertEquals(expect, DateUtils.fromString("2011/05/12 EDT 12:45:27", "yyyy/MM/dd z HH:mm:ss"));
    }
    @Test (expected=DateFormatException.class)
    public void fromString_format_wrongformat() {
    	DateUtils.fromString("2011/05/12 EST 12:45:27", "yyyy-MM-dd z HH:mm:ss");
    }
    
    @Test (expected=IllegalArgumentException.class)
    public void fromString_format_invalidformat() {
    	DateUtils.fromString("2011/05/12 EST 12:45:27", "yyyy/MM/dd z HH:mm:invalid");
    }

    @Test (expected=NullPointerException.class)
    public void fromString_format_noformat() {
    	DateUtils.fromString("2011/05/12 EST 12:45:27", null);
    }
  
    @Test (expected=DateFormatException.class)
    public void fromString_format_nodate() {
    	DateUtils.fromString(null, "yyyy/MM/dd z HH:mm:invalid");
    }
  
    /**
     * public static Date fromString(String string)
     */
    @Test
    public void fromString_noformat_knownformat() {
    	DateUtils.fromString("2011-05-12 12:45:27");
    }

    @Test (expected=DateFormatException.class)
    public void fromString_noformat_unformat() {
    	DateUtils.fromString("2011/05/12 12:45:27");
    }

    @Test (expected=DateFormatException.class)
    public void fromString_noformat_nodate() {
    	DateUtils.fromString(null);
    }

    /**
     * public static String toString(Date date, String format)
     */
    @Test
    public void toString_format_builtinformat() throws ParseException {
    	Date input =  new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse("2011/05/12 12:45:27");
    	assertEquals("2011-05-12 12:45:27", DateUtils.toString(input, "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    public void toString_format_newformat() throws ParseException {
    	Date input =  new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse("2011/05/12 12:45:27");
    	assertEquals("2011/05/12 12:45:27", DateUtils.toString(input, "yyyy/MM/dd HH:mm:ss"));
    }

    @Test
    public void toString_format_nulldate() throws ParseException {
    	assertEquals(null, DateUtils.toString(null, "yyyy-MM-dd HH:mm:ss"));
    }

    @Test (expected=NullPointerException.class)
    public void toString_format_nullformat() throws ParseException {
    	Date input =  new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse("2011/05/12 12:45:27");
    	assertEquals(null, DateUtils.toString(input, null));
    }

    /**
     * public static String toString(Date date)
     */
    @Test
    public void toString_noformat_builtinformat() throws ParseException {
    	Date input =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/12 EST 12:45:27");
    	assertEquals("2011-05-12T17:45:27Z", DateUtils.toString(input));
    }

    @Test
    public void toString_noformat_nulldate() throws ParseException {
    	assertEquals(null, DateUtils.toString(null));
    }

    /**
     * public static String toLabel(Date date, boolean isShortened)
     * Removed from testing because the behavior of "zz" is ambiguous, can produce either [EST] or [-4:00].
     */
    public void toLabel_unshortened() {
    	Date input = new Date(new Date().getTime() - (30 * SECOND));
    	String expect = new SimpleDateFormat("EEE, MMM d, yyyy, h:mm a zz").format(input);
    	assertEquals(expect, DateUtils.toLabel(new Date(new Date().getTime() - (30 * SECOND)), false));
    }

    @Test
    public void toLabel_underminute() {
    	assertEquals("less than a minute ago", DateUtils.toLabel(new Date(new Date().getTime() - (30 * SECOND)), true));
    }

    @Test
    public void toLabel_oneminute() {
    	assertEquals("1 minute ago", DateUtils.toLabel(new Date(new Date().getTime() - (65 * SECOND)), true));
    }

    @Test
    public void toLabel_underhour() {
    	assertEquals("5 minutes ago", DateUtils.toLabel(new Date(new Date().getTime() - ((5 * MINUTE) + (10 * SECOND))), true));
    }

    @Test
    public void toLabel_onehour() {
    	assertEquals("1 hour ago", DateUtils.toLabel(new Date(new Date().getTime() - (65 * MINUTE)), true));
    }

    @Test
    public void toLabel_underday() {
    	assertEquals("5 hours ago", DateUtils.toLabel(new Date(new Date().getTime() - ((5 * HOUR) + (10 * MINUTE))), true));
    }

    /**
     * Removed from testing because the behavior of "zz" is ambiguous, can produce either [EST] or [-4:00].
     * @throws ParseException
     */
    public void toLabel_overday() throws ParseException {
    	Date input =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	String expect = new SimpleDateFormat("EEE, MMM d, yyyy, h:mm a zz").format(input);
    	assertEquals(expect, DateUtils.toLabel(input, true));
    }

    @Test
    public void toLabel_null() {
    	assertEquals(null, DateUtils.toLabel(null, true));
    }

    
    /**
     * public static String toLabel(Date date)
     */
    @Test // Same as toLabel(date, true)
    public void toLabel() {
    	assertEquals("less than a minute ago", DateUtils.toLabel(new Date(new Date().getTime() - (30 * SECOND))));
    }
    
    
    /**
     * public static boolean isSameDay(Date a, Date b)
     */
    @Test (expected=NullPointerException.class)
    public void toLabel_null_first() {
    	assertEquals(null, DateUtils.isSameDay(null, new Date()));
    }

    @Test (expected=NullPointerException.class)
    public void toLabel_null_second() {
    	assertEquals(null, DateUtils.isSameDay(null, new Date()));
    }

    @Test
    public void toLabel_sameday_first_less() throws ParseException {
    	Date first =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 2:45:27");
    	Date second =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	assertEquals(true, DateUtils.isSameDay(first, second));
    }

    @Test
    public void toLabel_sameday_second_less() throws ParseException {
    	Date first =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	Date second =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 2:45:27");
    	assertEquals(true, DateUtils.isSameDay(first, second));
    }

    @Test
    public void toLabel_differentday_first_less() throws ParseException {
    	Date first =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/04/10 EST 12:45:27");
    	Date second =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	assertEquals(false, DateUtils.isSameDay(first, second));
    }

    @Test
    public void toLabel_differentday_second_less() throws ParseException {
    	Date first =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	Date second =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/04/10 EST 12:45:27");
    	assertEquals(false, DateUtils.isSameDay(first, second));
    }

    @Test
    public void toLabel_same_exact() throws ParseException {
    	Date first =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	Date second =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 12:45:27");
    	assertEquals(true, DateUtils.isSameDay(first, second));
    }

    @Test
    public void toLabel_midnight() throws ParseException {
    	Date first =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/09 EST 24:00:00");
    	Date second =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 24:00:00");
    	assertEquals(false, DateUtils.isSameDay(first, second));
    }
 
    
    /**
     * public static Date addTime(Date date, int field, int amount)
     */
    @Test (expected=NullPointerException.class)
    public void addTime_null() throws ParseException {
    	DateUtils.addTime(null, 0, 0);
    }

    @Test
    public void toLabel_day() throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/09 EST 05:00:00");
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 05:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.DAY_OF_MONTH, 1));
    }

    @Test
    public void toLabel_day_nextmonth() throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/31 EST 05:00:00");
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/06/01 EST 05:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.DAY_OF_MONTH, 1));
    }

    @Test
    public void toLabel_year() throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/09 EST 05:00:00");
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2012/05/09 EST 05:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.YEAR, 1));
    }

    @Test
    public void toLabel_month() throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/10 EST 05:00:00");
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/06/10 EST 05:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.MONTH, 1));
    }

    @Test
    public void toLabel_month_nextyear() throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/12/09 EST 05:00:00");
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2012/01/09 EST 05:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.MONTH, 1));
    }

    @Test
    public void toLabel_dow() throws ParseException {
    	Date start =  new SimpleDateFormat("E yyyy/MM/dd z HH:mm:ss").parse("Tue 2011/05/09 EST 24:00:00");
    	Date expect =  new SimpleDateFormat("E yyyy/MM/dd z HH:mm:ss").parse("Wed 2011/05/10 EST 24:00:00");
    	System.out.println(new SimpleDateFormat("E yyyy/MM/dd z HH:mm:ss").format(expect));
    	assertEquals(expect, DateUtils.addTime(start, Calendar.DAY_OF_WEEK, 1));
    }

    @Test
    public void toLabel_dow_nextweek() throws ParseException {
    	Date start =  new SimpleDateFormat("E yyyy/MM/dd z HH:mm:ss").parse("Sat 2011/05/13 EST 24:00:00");
    	Date expect =  new SimpleDateFormat("E yyyy/MM/dd z HH:mm:ss").parse("Mon 2011/05/15 EST 24:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.DAY_OF_WEEK, 2));
    }
    
    @Test
    public void toLabel_negative() throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/13 EST 24:00:00");
    	Date expect =  new SimpleDateFormat("yyyy/MM/dd z HH:mm:ss").parse("2011/05/11 EST 24:00:00");
    	assertEquals(expect, DateUtils.addTime(start, Calendar.DAY_OF_MONTH, -2));
    }

    
    /**
     * public static String toSimpleElapsedTime(Date date)
     */
    @Test
    public void toSimpleElapsedTime_underminute() {
    	assertEquals("less than a minute ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - (30 * SECOND))));
    }

    @Test
    public void toSimpleElapsedTime_oneminute() {
    	assertEquals("1 minute ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - (65 * SECOND))));
    }

    @Test
    public void toSimpleElapsedTime_underhour() {
    	assertEquals("5 minutes ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - ((5 * MINUTE) + (10 * SECOND)))));
    }

    @Test
    public void toSimpleElapsedTime_onehour() {
    	assertEquals("1 hour ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - (65 * MINUTE))));
    }

    @Test
    public void toSimpleElapsedTime_underday() {
    	assertEquals("5 hours ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - ((5 * HOUR) + (10 * MINUTE)))));
    }

    @Test
    public void toSimpleElapsedTime_oneday() throws ParseException {
    	assertEquals("1 day ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - ((1 * DAY) + (10 * MINUTE)))));
    }

    @Test
    public void toSimpleElapsedTime_multiday() throws ParseException {
    	assertEquals("5 days ago", DateUtils.toSimpleElapsedTime(new Date(new Date().getTime() - ((5 * DAY) + (10 * MINUTE)))));
    }

    @Test (expected=NullPointerException.class)
    public void toSimpleElapsedTime_null() {
    	assertEquals(null, DateUtils.toSimpleElapsedTime(null));
    }
}
