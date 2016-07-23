package com.psddev.dari.db.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class NumberModel extends Model<NumberModel, Double> {

    @Indexed
    private Double one;

    @Indexed
    private Set<Double> set;

    @Indexed
    private List<Double> list;

    @Indexed
    private NumberModel referenceOne;

    @Indexed
    private Set<NumberModel> referenceSet;

    @Indexed
    private List<NumberModel> referenceList;

    @Override
    public Double getOne() {
        return one;
    }

    @Override
    public void setOne(Double one) {
        this.one = one;
    }

    @Override
    public Set<Double> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<Double> set) {
        this.set = set;
    }

    @Override
    public List<Double> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<Double> list) {
        this.list = list;
    }

    @Override
    public NumberModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(NumberModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<NumberModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<NumberModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<NumberModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<NumberModel> referenceList) {
        this.referenceList = referenceList;
    }
}
