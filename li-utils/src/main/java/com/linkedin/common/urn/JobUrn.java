package com.linkedin.common.urn;

import java.net.URISyntaxException;

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

    public static JobUrn createFromString(String rawUrn) throws URISyntaxException {
        String jobName = new Urn(rawUrn).getContent();
        return new JobUrn(jobName);
    }

    public static JobUrn deserialize(String rawUrn) throws URISyntaxException {
        return createFromString(rawUrn);
    }
}
