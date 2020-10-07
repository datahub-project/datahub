package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;

import static com.linkedin.common.urn.UrnUtils.toFabricType;

public class DataProcessUrn extends Urn {
    public static final String ENTITY_TYPE = "dataProcess";

    private final String nameEntity;

    private static final String CONTENT_FORMAT = "(%s,%s,%s)";

    private final String orchestrator;

    private final FabricType originEntity;

    public DataProcessUrn(String orchestrator, String name, FabricType origin) {
        super(ENTITY_TYPE, String.format(CONTENT_FORMAT, orchestrator, name, origin.name()));
        this.orchestrator = orchestrator;
        this.nameEntity = name;
        this.originEntity = origin;
    }

    public String getNameEntity() {
        return nameEntity;
    }

    public String getOrchestrator() {
        return orchestrator;
    }

    public FabricType getOriginEntity() {
        return originEntity;
    }

    public static DataProcessUrn createFromString(String rawUrn) throws URISyntaxException {
        String content = new Urn(rawUrn).getContent();
        String[] parts = content.substring(1, content.length() - 1).split(",");
        return new DataProcessUrn(parts[0], parts[1], toFabricType(parts[2]));
    }

    public static DataProcessUrn deserialize(String rawUrn) throws URISyntaxException {
        return createFromString(rawUrn);
    }

    static {
        Custom.registerCoercer(new DirectCoercer<DataProcessUrn>() {
            public Object coerceInput(DataProcessUrn object) throws ClassCastException {
                return object.toString();
            }

            public DataProcessUrn coerceOutput(Object object) throws TemplateOutputCastException {
                try {
                    return DataProcessUrn.createFromString((String) object);
                } catch (URISyntaxException e) {
                    throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
                }
            }
        }, DataProcessUrn.class);
    }
}
