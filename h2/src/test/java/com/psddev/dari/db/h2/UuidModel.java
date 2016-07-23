package com.psddev.dari.db.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class UuidModel extends Model<UuidModel, UUID> {

    @Indexed
    private UUID one;

    @Indexed
    private Set<UUID> set;

    @Indexed
    private List<UUID> list;

    @Indexed
    private UuidModel referenceOne;

    @Indexed
    private Set<UuidModel> referenceSet;

    @Indexed
    private List<UuidModel> referenceList;

    @Override
    public UUID getOne() {
        return one;
    }

    @Override
    public void setOne(UUID one) {
        this.one = one;
    }

    @Override
    public Set<UUID> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<UUID> set) {
        this.set = set;
    }

    @Override
    public List<UUID> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<UUID> list) {
        this.list = list;
    }

    @Override
    public UuidModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(UuidModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<UuidModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<UuidModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<UuidModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<UuidModel> referenceList) {
        this.referenceList = referenceList;
    }
}
