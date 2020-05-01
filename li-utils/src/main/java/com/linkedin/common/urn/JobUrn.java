package com.linkedin.common.urn;

import com.linkedin.common.FabricType;

import java.net.URISyntaxException;

import static com.linkedin.common.urn.UrnUtils.toFabricType;

public class JobUrn extends Urn{
    public static final String ENTITY_TYPE = "job";

    private final String jobNameEntity;

    private static final String CONTENT_FORMAT = "(%s,%s,%s)";

    private final DataPlatformUrn platformEntity;

    private final FabricType originEntity;

    public JobUrn(DataPlatformUrn platform, String name, FabricType origin) {
        super(ENTITY_TYPE, String.format(CONTENT_FORMAT, platform.toString(), name, origin.name()));
        this.platformEntity = platform;
        this.jobNameEntity = name;
        this.originEntity = origin;
    }

    public String getJobNameEntity() {
        return jobNameEntity;
    }

    public static JobUrn createFromString(String rawUrn) throws URISyntaxException {
        String content = new Urn(rawUrn).getContent();
        String[] parts = content.substring(1, content.length()-1).split(",");
        return new JobUrn(DataPlatformUrn.createFromString(parts[0]), parts[1], toFabricType(parts[2]));
    }

    public static JobUrn deserialize(String rawUrn) throws URISyntaxException {
        return createFromString(rawUrn);
    }
}
