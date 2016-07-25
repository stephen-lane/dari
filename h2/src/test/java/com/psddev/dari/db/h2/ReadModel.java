package com.psddev.dari.db.h2;

import com.psddev.dari.db.Record;

public class ReadModel extends Record {

    @Indexed(unique = true)
    @Required
    public String text;

    @Indexed
    public String getFirstLetter() {
        return text.substring(0, 1);
    }
}
