package com.psddev.dari.db.h2;

import com.psddev.dari.db.Region;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RegionModel extends Model<RegionModel, Region> {

    @Indexed
    private Region one;

    @Indexed
    private Set<Region> set;

    @Indexed
    private List<Region> list;

    @Indexed
    private RegionModel referenceOne;

    @Indexed
    private Set<RegionModel> referenceSet;

    @Indexed
    private List<RegionModel> referenceList = new ArrayList<>();

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
    public RegionModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(RegionModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<RegionModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<RegionModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<RegionModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<RegionModel> referenceList) {
        this.referenceList = referenceList;
    }
}
