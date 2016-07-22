package com.psddev.dari.db.h2;

import com.psddev.dari.db.Record;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Model<M extends Model<M, T>, T> extends Record {

    @Indexed
    public T field;

    @Indexed
    public final Set<T> set = new LinkedHashSet<>();

    @Indexed
    public final List<T> list = new ArrayList<>();

    @Indexed
    public Model<?, ?> referenceField;

    @Indexed
    public final Set<Model<?, ?>> referenceSet = new LinkedHashSet<>();

    @Indexed
    public final List<Model<?, ?>> referenceList = new ArrayList<>();
}
