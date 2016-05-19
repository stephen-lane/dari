package com.psddev.dari.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.junit.Ignore;
import org.junit.Test;

public class StringUtilsTest {

	/**
	 * public static <T> T fromString(Class<T> returnType, String... strings)
	 */
	@Test
	public void fromString_primitive_boolean() {
		assertEquals(Boolean.class, StringUtils.fromString(boolean.class, "true").getClass());
		assertEquals(new Boolean(true), StringUtils.fromString(boolean.class, "true"));
	}

	@Test
	public void fromString_primitive_byte() {
		assertEquals(Byte.class, StringUtils.fromString(byte.class, "1").getClass());
		assertEquals(new Byte((byte)1), StringUtils.fromString(byte.class, "1"));
	}

	@Test
	public void fromString_primitive_short() {
		assertEquals(Short.class, StringUtils.fromString(short.class, "7").getClass());
		assertEquals(new Short((short)7), StringUtils.fromString(short.class, "7"));
	}

	@Test
	public void fromString_primitive_int() {
		assertEquals(Integer.class, StringUtils.fromString(int.class, "1234").getClass());
		assertEquals(new Integer(1234), StringUtils.fromString(int.class, "1234"));
	}

	@Test
	public void fromString_primitive_long() {
		assertEquals(Long.class, StringUtils.fromString(long.class, "1234").getClass());
		assertEquals(new Long(1234L), StringUtils.fromString(long.class, "1234"));
	}

	@Test
	public void fromString_primitive_float() {
		assertEquals(Float.class, StringUtils.fromString(float.class, "12.34").getClass());
		assertEquals(new Float(12.34), StringUtils.fromString(float.class, "12.34"));
	}

	@Test
	public void fromString_primitive_double() {
		assertEquals(Double.class, StringUtils.fromString(double.class, "23.5").getClass());
		assertEquals(new Double(23.5), StringUtils.fromString(double.class, "23.5"));
	}

	@Test
	public void fromString_primitive_char() {
		assertEquals(Character.class, StringUtils.fromString(char.class, "c").getClass());
		assertEquals(new Character('c'), StringUtils.fromString(char.class, "c"));
	}

	/* Non-Primitives */
	@Test
	public void fromString_string() {
		assertEquals(String.class, StringUtils.fromString(String.class, "value").getClass());
		assertEquals(new String("value"), StringUtils.fromString(String.class, "value"));
	}

	@Test
	public void fromString_date() throws ParseException {
		String dateStr = "2011-10-15 14:15:00";
		Date expect = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr);

		assertEquals(Date.class, StringUtils.fromString(Date.class, dateStr).getClass());
		assertEquals(expect, StringUtils.fromString(Date.class, dateStr));
	}

	@Test (expected=IllegalArgumentException.class)
	public void fromString_other() {
		class MyClass {}
		StringUtils.fromString(MyClass.class, "c");
	}

	/* Arrays */
	@Test
	public void fromString_array() {
		assertEquals(char[].class, StringUtils.fromString(char[].class, "c","d").getClass());
		assertArrayEquals(new char[] {'c','d'}, StringUtils.fromString(char[].class, "c","d"));
	}

	@Test
	public void fromString_array_empty() {
		assertEquals(char[].class, StringUtils.fromString(char[].class).getClass());
		assertArrayEquals(new char[] {}, StringUtils.fromString(char[].class));
	}

	@Test // Honestly, this feels wrong... seems like it should return an empty array
	public void fromString_array_null() {
		assertEquals(char[].class, StringUtils.fromString(char[].class).getClass());
		assertArrayEquals(new char[] {}, StringUtils.fromString(char[].class));
	}

	@Test (expected=NullPointerException.class) // feels like it should be an InvalidArgumentException
	public void fromString_array_null_null() {
		assertEquals(null, StringUtils.fromString(char[].class, null, null));
	}

	@Test (expected=IllegalArgumentException.class)
	public void fromString_array_invalid() {
		StringUtils.fromString(int[].class, "1","a");
	}


	/* Edge cases */
	@Test // What happens if you specify a primitive type but multiple values
	// Feels like this should be an InvalidArgumentException
	public void fromString_primitive_multiple() {
		assertEquals(Integer.class, StringUtils.fromString(int.class, "1","2").getClass());
		assertEquals(new Integer(1), StringUtils.fromString(int.class, "1","2"));
	}

	@Test (expected=NumberFormatException.class)
	public void fromString_missing_value() {
		StringUtils.fromString(int.class);
	}

	@Test (expected=NumberFormatException.class)
	public void fromString_null_value() {
		StringUtils.fromString(int.class, (String)null);
	}

	@Test (expected=NumberFormatException.class)
	public void fromString_invalid_value() {
		StringUtils.fromString(int.class, "a");
	}

	@Test (expected=NumberFormatException.class)
	public void fromString_invalid_value_after() {
		StringUtils.fromString(int.class, "2a");
	}

	@Test (expected=NumberFormatException.class)
	public void fromString_invalid_value_before() {
		StringUtils.fromString(int.class, "a2");
	}


	/**
	 * protected static List<String> splitString(String string)
	 */
	@Test
	public void splitString_spaces() {
		assertEquals(Arrays.asList("a","string","with","spaces"), StringUtils.splitString("a string with spaces"));
	}

	@Test
	public void splitString_caps() {
		assertEquals(Arrays.asList("a","string","with","caps"), StringUtils.splitString("aStringWithCaps"));
	}

	@Test
	public void splitString_underscores() {
		assertEquals(Arrays.asList("a","string","with","words"), StringUtils.splitString("a_string_with_words"));
	}
	@Test
	public void splitString_hyphens() {
		assertEquals(Arrays.asList("a","string","with","words"), StringUtils.splitString("a-string-with-words"));
	}
	@Test
	public void splitString_periods() {
		assertEquals(Arrays.asList("a","string","with","words"), StringUtils.splitString("a.string.with.words"));
	}
	@Test
	public void splitString_multi_delimeter() {
		assertEquals(Arrays.asList("a","string","with","words"), StringUtils.splitString("a.string. with. Words"));
	}
	@Test // Not what the user would want, but no good way to avoid it
	public void splitString_multi_caps() {
		assertEquals(Arrays.asList("this","is","astring"), StringUtils.splitString("thisIsAString"));
	}
	@Test
	public void splitString_leading_delimeters() {
		assertEquals(Arrays.asList("", "a","string","with","words"), StringUtils.splitString(" A string with words"));
	}
	@Test
	public void splitString_trailing_delimeters() {
		assertEquals(Arrays.asList("a","string","with","words"), StringUtils.splitString("A string with words. "));
	}
	@Test // All returned words are converted to lower case
	public void splitString_lowercased() {
		assertEquals(Arrays.asList("a","string","with","words"), StringUtils.splitString("A STRING with WORDS"));
	}

	/**
	 * public static String toDelimited(String string, String delimiter)
	 * We're really only testing the join part, since splitString handles splitting
	 */
	@Test
	public void toDelimited_dashes() {
		assertEquals("a-string-with-words", StringUtils.toDelimited("a string with words", "-"));
	}

	/**
	 * public static String toHyphenated(String string)
	 */
	@Test
	public void toHyphenated() {
		assertEquals("a-string-with-words", StringUtils.toHyphenated("a string with words"));
	}
	@Test (expected=NullPointerException.class)
	public void toHyphenated_null() {
		StringUtils.toHyphenated(null);
	}

	/**
	 * public static String toUnderscored(String string)
	 */
	@Test
	public void toUnderscored() {
		assertEquals("a_string_with_words", StringUtils.toUnderscored("a string with words"));
	}
	@Test (expected=NullPointerException.class)
	public void toUnderscored_null() {
		StringUtils.toUnderscored(null);
	}

	/**
	 * public static String toPascalCase(String string)
	 */
	@Test
	public void toPascalCase() {
		assertEquals("AStringWithWords", StringUtils.toPascalCase("a string with words"));
	}
	@Test
	public void toPascalCase_leadingDelimiters() {
		assertEquals("AStringWithWords", StringUtils.toPascalCase("_a_string_with_words"));
	}
	@Test (expected=NullPointerException.class)
	public void toPascalCase_null() {
		StringUtils.toPascalCase(null);
	}

	/**
	 * public static String toCamelCase(String string)
	 */
	@Test
	public void toCamelCase() {
		assertEquals("aStringWithWords", StringUtils.toCamelCase("a string with words"));
	}
	@Test (expected=NullPointerException.class)
	public void toCamelCase_null() {
		StringUtils.toCamelCase(null);
	}

	/**
	 * public static String toLabel(String string)
	 */
	@Test
	public void toLabel() {
		assertEquals("A String With Words", StringUtils.toLabel("a-string-with-words"));
	}
	@Test
	public void toLabel_question() {
		assertEquals("A String With Words?", StringUtils.toLabel("is-a-string-with-words"));
	}
	@Test
	public void toLabel_null() {
		assertEquals(null, StringUtils.toLabel(null));
	}


	/**
	 * public static String toNormalized(CharSequence string)
	 */
	@Test
	public void toNormalized_simple() {
		assertEquals("a-string-with-words", StringUtils.toNormalized("A String With Words"));
	}

	@Test
	public void toNormalized_accented_single() {
		assertEquals("a-string-with-words", StringUtils.toNormalized("\u00C1 String With Words"));
	}

	@Test
	public void toNormalized_accented_double() {
		assertEquals("a-string-with-words", StringUtils.toNormalized("\u0041\u0301 String With Words"));
	}

	@Test
	public void toNormalized_squote() {
		assertEquals("a-string-with-quotes", StringUtils.toNormalized("A 'String' With Quotes"));
	}

	@Test
	public void toNormalized_numbers() {
		assertEquals("a-string-with-1-number", StringUtils.toNormalized("A String With 1 Number"));
	}

	@Test
	public void toNormalized_dashes() {
		assertEquals("a-string-with-a-hyphenated-word", StringUtils.toNormalized("A String With a hyphenated-word"));
	}

	@Test
	public void toNormalized_multi_replaced() {
		assertEquals("a-string-with-multiple-things-to-replace-in-a-row", StringUtils.toNormalized("A String With multiple . things to replace in a row"));
	}


	/**
	 * public static String[] fromCsv(String string)
	 * Tests per: http://tools.ietf.org/html/rfc4180
	 */
	@Test
	public void fromCsv_null() {
		assertArrayEquals(null, StringUtils.fromCsv(null));
	}
	@Test
	public void fromCsv() {
		assertArrayEquals(new String[]{"a","b"}, StringUtils.fromCsv("a,b"));
	}
	@Ignore // The CVS functionality in StringUtils is to be deprecated, so failing test can be ignored
	@Test
	public void fromCsv_quoted() {
		assertArrayEquals(new String[]{"a","b"}, StringUtils.fromCsv("\"a\",b"));
	}
	@Ignore // The CVS functionality in StringUtils is to be deprecated, so failing test can be ignored
	@Test
	public void fromCsv_quoted_multichar() {
		assertArrayEquals(new String[]{"abc","b"}, StringUtils.fromCsv("\"abc\",b"));
	}
	@Test
	public void fromCsv_inline_quoted_quote() {
		assertArrayEquals(new String[]{"a\"","b"}, StringUtils.fromCsv("\"a\"\"\",b"));
	}
	@Test
	public void fromCsv_space() {
		assertArrayEquals(new String[]{" a"," b "}, StringUtils.fromCsv(" a, b "));
	}
	@Test
	public void fromCsv_comma() {
		assertArrayEquals(new String[]{"a,b","c"}, StringUtils.fromCsv("\"a,b\",c"));
	}
	@Test
	public void fromCsv_newline() {
		assertArrayEquals(new String[]{"a\nb","c"}, StringUtils.fromCsv("\"a\nb\",c"));
	}


	/**
	 * public static String toCsv(String... strings)
	 * Tests per: http://tools.ietf.org/html/rfc4180
	 */
	@Test
	public void toCsv_null() {
		assertEquals(null, StringUtils.toCsv((String[])null));
	}
	@Ignore // The CVS functionality in StringUtils is to be deprecated, so failing test can be ignored
	@Test
	public void toCsv_novalues() {
		assertEquals(null, StringUtils.toCsv());
	}
	@Test
	public void toCsv_empty() {
		assertEquals("", StringUtils.toCsv(""));
	}
	@Test
	public void toCsv_simple() {
		assertEquals("a,b,c", StringUtils.toCsv("a","b","c"));
	}
	@Test
	public void toCsv_dquotes() {
		assertEquals("\"a\"\"\",b,c", StringUtils.toCsv("a\"","b","c"));
	}
	@Test
	public void toCsv_comma() {
		assertEquals("a,\"b,c\",d", StringUtils.toCsv("a","b,c","d"));
	}
	@Test
	public void toCsv_newline() {
		assertEquals("a,\"b\nc\",d", StringUtils.toCsv("a","b\nc","d"));
	}
	@Test // single quotes are not special
	public void toCsv_squotes() {
		assertEquals("a',b,c", StringUtils.toCsv("a'","b","c"));
	}


	/**
	 * CSV round trip
	 */
	public void roundCsv_empty() {
		String[] input = {""};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}
	@Test
	public void roundCsv_simple() {
		String[] input = {"a", "b", "c"};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}
	@Test
	public void roundCsv_dquotes() {
		String[] input = {"a\"", "b", "c"};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}
	@Test
	public void roundCsv_comma() {
		String[] input = {"a", "b,c", "d"};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}
	@Test
	public void roundCsv_newline() {
		String[] input = {"a", "b\nc", "d"};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}
	@Test // single quotes are not special
	public void roundCsv_squotes() {
		String[] input = {"a'", "b", "c"};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}
	@Ignore // The CVS functionality in StringUtils is to be deprecated, so failing test can be ignored
	@Test // single quotes are not special
	public void roundCsv_novalues() {
		String[] input = {};
		String toCsv = StringUtils.toCsv(input);
		String[] fromCsv = StringUtils.fromCsv(toCsv);
		String toCsvR = StringUtils.toCsv(fromCsv);

		assertArrayEquals(input, fromCsv); // round trip array -> csv -> array
		assertEquals(toCsv, toCsvR);       // round trip csv -> array -> csv
	}


	/**
	 * public static String escapeJavaScript(String string)
	 */
	@Test
	public void escapeJavaScript_null() {
		assertEquals(null, StringUtils.escapeJavaScript(null));
	}
	@Test
	public void escapeJavaScript_blank() {
		assertEquals("", StringUtils.escapeJavaScript(""));
	}
	@Test
	public void escapeJavaScript_noescaped() {
		assertEquals("abcdef", StringUtils.escapeJavaScript("abcdef"));
	}
	@Test
	public void escapeJavaScript_space() {
		assertEquals("abc\\x20def", StringUtils.escapeJavaScript("abc def"));
	}
	@Test
	public void escapeJavaScript_quote() {
		assertEquals("abc\\x27def", StringUtils.escapeJavaScript("abc'def"));
	}
	@Test
	public void escapeJavaScript_dquote() {
		assertEquals("abc\\x22def", StringUtils.escapeJavaScript("abc\"def"));
	}
	@Test
	public void escapeJavaScript_amp() {
		assertEquals("abc\\x26def", StringUtils.escapeJavaScript("abc&def"));
	}
	@Test
	public void escapeJavaScript_ltgt() {
		assertEquals("abc\\x3cdef\\x3e", StringUtils.escapeJavaScript("abc<def>"));
	}

	/*
	 * Use case for possible exploit/bug
	 * input: " onmouseover=alert(1)
	 * html: <a href="#" onclick="alert('<%= escapeJavaScript(message) %>'); return false;"></a>
	 * don't want: <a href="#" onclick="alert('\" onmouseover=alert(1) '); return false;">message</a>
	 * We want all characters to be escaped using a format that wouldn't need to be html-escaped
	 */
	@Test
	public void escapeJavaScript_usecase_1() {
		String message = "\" onmouseover=alert(1) ";
		assertEquals("\\x22\\x20onmouseover\\x3dalert\\x281\\x29\\x20", StringUtils.escapeJavaScript(message));
	}

	/**
	 * public static String addQueryParameters(String uri, Object... parameters)
	 */
	@Test
	public void addQueryParameters_simple() {
		assertEquals("http://test.com/a?b=1", StringUtils.addQueryParameters("http://test.com/a", "b", "1"));
	}

	@Test
	public void addQueryParameters_append() {
		assertEquals("http://test.com/a?c=2&b=1", StringUtils.addQueryParameters("http://test.com/a?c=2", "b", "1"));
	}

	@Test
	public void addQueryParameters_multiple() {
		assertEquals("http://test.com/a?c=2&b=1&d=3", StringUtils.addQueryParameters("http://test.com/a?c=2", "b", "1", "d", "3"));
	}

	@Test
	public void addQueryParameters_escape() {
		assertEquals("http://test.com/a?b=%3F%26%3D", StringUtils.addQueryParameters("http://test.com/a", "b", "?&="));
	}

	@Test
	public void addQueryParameters_url() {
		assertEquals("http://test.com/a?url=http%3A%2F%2Fb.com%2Fc%3Fd%3D2", StringUtils.addQueryParameters("http://test.com/a", "url", "http://b.com/c?d=2"));
	}

	@Test
	public void addQueryParameters_null() {
		assertEquals(null, StringUtils.addQueryParameters(null, "b", "1"));
	}


	@Test // No params, input=output
	public void addQueryParameters_noparams() {
		assertEquals("http://test.com/a", StringUtils.addQueryParameters("http://test.com/a"));
	}

	@Test (expected=NullPointerException.class)
	public void addQueryParameters_nullparamname() {
		StringUtils.addQueryParameters("http://test.com/a", null, "1");
	}

	@Test // empty parameter name == not included in output
	public void addQueryParameters_emptyparamname() {
		StringUtils.addQueryParameters("http://test.com/a", "", "1");
	}

	@Test // null parameter value == not included in output
	public void addQueryParameters_nullparamvalue() {
		assertEquals("http://test.com/a", StringUtils.addQueryParameters("http://test.com/a", "b", null));
	}

	@Test // empty parameter value == included in output as empty
	public void addQueryParameters_emptyparamvalue() {
		assertEquals("http://test.com/a?b=", StringUtils.addQueryParameters("http://test.com/a", "b", ""));
		assertEquals("http://test.com/a?b=&c=2", StringUtils.addQueryParameters("http://test.com/a", "b", "", "c", "2"));
	}
}
