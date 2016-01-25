package com.psddev.dari.util;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java8.util.Spliterator;
import java8.util.Spliterators;
import java8.util.function.BiConsumer;
import java8.util.function.Consumer;
import java8.util.function.Function;
import java8.util.function.Predicate;
import java8.util.stream.Stream;
import java8.util.stream.StreamSupport;

public class StreamSupportUtils {

    public static <T> void forEach(Iterable<? extends T> iterable, Consumer<? super T> action) {
        Objects.requireNonNull(action);
        for (T t : iterable) {
            action.accept(t);
        }
    }

    public static <K, V> void mapForEach(Map<K, V> map, BiConsumer<? super K, ? super V> action) {
        Objects.requireNonNull(action);
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K k;
            V v;
            try {
                k = entry.getKey();
                v = entry.getValue();
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                throw new ConcurrentModificationException(ise);
            }
            action.accept(k, v);
        }
    }

    public static <E> boolean removeIf(Iterable<E> iterable, Predicate<? super E> filter) {
        Objects.requireNonNull(filter);
        boolean removed = false;
        final Iterator<E> each = iterable.iterator();
        while (each.hasNext()) {
            if (filter.test(each.next())) {
                each.remove();
                removed = true;
            }
        }
        return removed;
    }

    public static <T> Stream<T> of(T... values) {
        return StreamSupport.stream(Arrays.asList(values));
    }

    public static Stream<String> splitAsStream(Pattern parttern, CharSequence input) {
        class MatcherIterator implements Iterator<String> {
            private final Matcher matcher;
            // The start position of the next sub-sequence of input
            // when current == input.length there are no more elements
            private int current;
            // null if the next element, if any, needs to obtained
            private String nextElement;
            // > 0 if there are N next empty elements
            private int emptyElementCount;

            MatcherIterator() {
                this.matcher = parttern.matcher(input);
            }

            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                if (emptyElementCount == 0) {
                    String n = nextElement;
                    nextElement = null;
                    return n;
                } else {
                    emptyElementCount--;
                    return "";
                }
            }

            public boolean hasNext() {
                if (nextElement != null || emptyElementCount > 0) {
                    return true;
                }

                if (current == input.length()) {
                    return false;
                }

                // Consume the next matching element
                // Count sequence of matching empty elements
                while (matcher.find()) {
                    nextElement = input.subSequence(current, matcher.start()).toString();
                    current = matcher.end();
                    if (!nextElement.isEmpty()) {
                        return true;
                    } else if (current > 0) { // no empty leading substring for zero-width
                                              // match at the beginning of the input
                        emptyElementCount++;
                    }
                }

                // Consume last matching element
                nextElement = input.subSequence(current, input.length()).toString();
                current = input.length();
                if (!nextElement.isEmpty()) {
                    return true;
                } else {
                    // Ignore a terminal sequence of matching empty elements
                    emptyElementCount = 0;
                    nextElement = null;
                    return false;
                }
            }
        }

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new MatcherIterator(), Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /**
     * Returns a function that always returns its input argument.
     *
     * @param <T> the type of the input and output objects to the function
     * @return a function that always returns its input argument
     */
    public static <T> Function<T, T> identity() {
        return t -> t;
    }
}
