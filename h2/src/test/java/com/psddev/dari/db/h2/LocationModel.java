package com.psddev.dari.db.h2;

import com.psddev.dari.db.Location;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LocationModel extends Model<LocationModel, Location> {

    @Indexed
    private Location one;

    @Indexed
    private Set<Location> set;

    @Indexed
    private List<Location> list;

    @Indexed
    private LocationModel referenceOne;

    @Indexed
    private Set<LocationModel> referenceSet;

    @Indexed
    private List<LocationModel> referenceList;

    @Override
    public Location getOne() {
        return one;
    }

    @Override
    public void setOne(Location one) {
        this.one = one;
    }

    @Override
    public Set<Location> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<Location> set) {
        this.set = set;
    }

    @Override
    public List<Location> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<Location> list) {
        this.list = list;
    }

    @Override
    public LocationModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(LocationModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<LocationModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<LocationModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<LocationModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<LocationModel> referenceList) {
        this.referenceList = referenceList;
    }
}
