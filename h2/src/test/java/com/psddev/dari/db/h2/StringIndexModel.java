package com.psddev.dari.db.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class StringIndexModel extends AbstractIndexModel<StringIndexModel, String> {

    @Indexed
    private String one;

    @Indexed
    private Set<String> set;

    @Indexed
    private List<String> list;

    @Indexed
    private StringIndexModel referenceOne;

    @Indexed
    private Set<StringIndexModel> referenceSet;

    @Indexed
    private List<StringIndexModel> referenceList;

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
    public StringIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(StringIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<StringIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<StringIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<StringIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<StringIndexModel> referenceList) {
        this.referenceList = referenceList;
    }
}
