package com.psddev.dari.util;

/**
 * Skeletal implementation of an image editor. Subclasses should further
 * implement the following:
 *
 * <ul>
 *     <li>{@link #edit}</li>
 *     <li>{@link #initialize}</li>
 * </ul>
 */
public abstract class AbstractImageEditor implements ImageEditor {

    private String name;

    // --- ImageEditor support ---

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    // --- Object support ---

    @Override
    public String toString() {
        return getName();
    }
}
