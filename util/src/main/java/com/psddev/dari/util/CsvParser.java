package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rhseeger on 4/13/15.
 */
public class CsvParser {
    public static final Character DEFAULT_FIELD_SEPARATOR = ',';
    public static final Character DEFAULT_QUOTE_CHARACTER = '"';

    public static class Static {
        public static String[] fromCsv(String string, Character fieldSeparator, Character quoteCharacter) {
            if(string == null) {
                return null;
            }
            if(string.length() == 0) {
                return new String[]{""};
            }

            String FS = Pattern.quote(fieldSeparator.toString());
            String QC = Pattern.quote(quoteCharacter.toString());
            String NL = Pattern.quote("\n");
            String CR = Pattern.quote("\r");
            String UNESCAPED = "[^" + FS + QC + NL + CR + "]*";
            String ESCAPED = "(?:[^" + QC + "]|(?:" + QC + QC + "))*";
            String EXPRESSION = "(" + UNESCAPED + ")|(?:" + QC + "(" + ESCAPED + ")" + QC + ")";

            Pattern first = Pattern.compile("^(" + EXPRESSION + ")(?:(" + FS + ").*)?$");
            Pattern more = Pattern.compile("^(" + FS + EXPRESSION + ")(?:" + FS + ".*)?$");

            List<String> result = new ArrayList<String>();

            String matched;
            Matcher matcher;

            int offset = 0;
            Pattern pattern = first;

            while(offset < string.length()) {
                matcher = pattern.matcher(string.substring(offset));
                //System.out.println("Matching [" + string.substring(offset) + "](" + matcher.matches() + ")");
                if (!matcher.matches()) {
                    //System.out.println("Could not parse input as CSV");
                    throw new CsvParsingException("Could not parse input as CSV");
                }

                if (matcher.group(2) == null) {
                    matched = matcher.group(3);
                } else if (matcher.group(3) == null) {
                    matched = matcher.group(2);
                } else {
                    matched = matcher.group(2).length() > matcher.group(3).length() ? matcher.group(2) : matcher.group(3);
                }
                result.add(matched.replaceAll(QC + QC, quoteCharacter.toString()));

                offset += matcher.group(1).length();
                pattern = more;
            }

            //System.out.println("CSV: line [" + string + "] -> [" + StringUtils.join(result, "],[") + "]");
            return result.toArray(new String[]{});
        }

    }

    // Putting an empty exception class here to be able to show where we might want to be throwing exceptions above
    public static class CsvParsingException extends RuntimeException {
        public CsvParsingException(String message) {
            super(message);
        }
    }

}
