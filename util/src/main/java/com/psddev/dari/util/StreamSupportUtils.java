package com.psddev.dari.util;

import java.util.Objects;

import java8.util.function.Consumer;

public class StreamSupportUtils {

    public static <T> void forEach(Iterable<? extends T> iterable, Consumer<? super T> action) {
        Objects.requireNonNull(action);
        for (T t : iterable) {
            action.accept(t);
        }
    }
}
