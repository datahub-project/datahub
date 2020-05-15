package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import java.net.URISyntaxException;

import static com.linkedin.common.urn.UrnUtils.toFabricType;

public class DataJobUrn extends Urn {
    public static final String ENTITY_TYPE = "dataJob";

    private final String dataJobNameEntity;

    private static final String CONTENT_FORMAT = "(%s,%s,%s)";

    private final String dataJobOrchestrator;

    private final FabricType originEntity;

    public DataJobUrn(String orchestrator, String name, FabricType origin) {
        super(ENTITY_TYPE, String.format(CONTENT_FORMAT, orchestrator, name, origin.name()));
        this.dataJobOrchestrator = orchestrator;
        this.dataJobNameEntity = name;
        this.originEntity = origin;
    }

    public String getJobNameEntity() {
        return dataJobNameEntity;
    }

    public String getDataJobOrchestrator() {
        return dataJobOrchestrator;
    }

    public FabricType getOriginEntity() {
        return originEntity;
    }

    public static DataJobUrn createFromString(String rawUrn) throws URISyntaxException {
        String content = new Urn(rawUrn).getContent();
        String[] parts = content.substring(1, content.length() - 1).split(",");
        return new DataJobUrn(parts[0], parts[1], toFabricType(parts[2]));
    }

    public static DataJobUrn deserialize(String rawUrn) throws URISyntaxException {
        return createFromString(rawUrn);
    }
}
