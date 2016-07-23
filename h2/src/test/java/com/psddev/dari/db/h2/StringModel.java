package com.psddev.dari.db.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class StringModel extends Model<StringModel, String> {

    @Indexed
    private String one;

    @Indexed
    private Set<String> set;

    @Indexed
    private List<String> list;

    @Indexed
    private StringModel referenceOne;

    @Indexed
    private Set<StringModel> referenceSet;

    @Indexed
    private List<StringModel> referenceList;

    @Override
    public String getOne() {
        return one;
    }

    @Override
    public void setOne(String one) {
        this.one = one;
    }

    @Override
    public Set<String> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<String> set) {
        this.set = set;
    }

    @Override
    public List<String> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<String> list) {
        this.list = list;
    }

    @Override
    public StringModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(StringModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<StringModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<StringModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<StringModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<StringModel> referenceList) {
        this.referenceList = referenceList;
    }
}
