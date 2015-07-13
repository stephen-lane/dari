package com.psddev.dari.db;

import com.psddev.dari.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/13/15.
 */
public class AssertUtils {

    /**
     * Asserts that the two lists include the same Records, since we can't rely on the order dari returns them in
     * if we don't sort
     * @param first
     * @param second
     * @param <T>
     */
    public static <T extends Record> void assertEqualsUnordered(List<T> first, List<T> second) {
        List<T> firstCopy = new ArrayList<T>();
        firstCopy.addAll(first);
        firstCopy.sort((T o1, T o2) -> compareById(o1, o2));

        List<T> secondCopy = new ArrayList<T>();
        secondCopy.addAll(second);
        secondCopy.sort((T o1, T o2) -> compareById(o1, o2));

        assertEquals(firstCopy, secondCopy);
    }

    public static <T extends Record> int compareById(T o1, T o2) {
        if(o1 == null && o2 == null) {
            return 0;
        }
        if(o1 == null) {
            return -1;
        }
        if(o2 == null) {
            return 1;
        }
        return ObjectUtils.compare(o1.getId(), o2.getId(), false);
    }
}
