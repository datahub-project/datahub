package com.linkedin.common.urn;

public class JobUrn extends Urn{
    public static final String ENTITY_TYPE = "job";

    private final String nameEntity;

    public JobUrn(String name) {
        super(ENTITY_TYPE, name);
        this.nameEntity = name;
    }

    public String getNameEntity() {
        return nameEntity;
    }
}
