package com.psddev.dari.db.h2;

import com.psddev.dari.db.Region;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RegionIndexModel extends AbstractIndexModel<RegionIndexModel, Region> {

    @Indexed
    private Region one;

    @Indexed
    private Set<Region> set;

    @Indexed
    private List<Region> list;

    @Indexed
    private RegionIndexModel referenceOne;

    @Indexed
    private Set<RegionIndexModel> referenceSet;

    @Indexed
    private List<RegionIndexModel> referenceList = new ArrayList<>();

    @Override
    public Region getOne() {
        return one;
    }

    @Override
    public void setOne(Region one) {
        this.one = one;
    }

    @Override
    public Set<Region> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<Region> set) {
        this.set = set;
    }

    @Override
    public List<Region> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<Region> list) {
        this.list = list;
    }

    @Override
    public RegionIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(RegionIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<RegionIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<RegionIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<RegionIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<RegionIndexModel> referenceList) {
        this.referenceList = referenceList;
    }
}
