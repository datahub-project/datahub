package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.springframework.core.io.ClassPathResource;


public class IngestDataPlatformsStep implements BootstrapStep {

    private static final String PLATFORM_ENTITY_NAME = "dataPlatform";
    private static final String PLATFORM_ASPECT_NAME = "dataPlatformInfo";

    private final EntityService _entityService;

    public IngestDataPlatformsStep(final EntityService entityService) {
        _entityService = entityService;
    }

    @Override
    public String name() {
        return "IngestDataPlatformsStep";
    }

    @Override
    public void execute() throws IOException, URISyntaxException {

        final ObjectMapper mapper = new ObjectMapper();

        // 1. Read from the file into JSON.
        final JsonNode dataPlatformsObj = mapper.readTree(new ClassPathResource("./boot/data_platforms.json").getFile());

        if (!dataPlatformsObj.isArray()) {
            throw new RuntimeException(String.format("Found malformed data platforms file, expected an Array but found %s", dataPlatformsObj.getNodeType()));
        }

        // 2. For each JSON object, cast into a DataPlatformSnapshot object.
        for (Iterator<JsonNode> it = dataPlatformsObj.iterator(); it.hasNext(); ) {
            final JsonNode dataPlatformObj = it.next();
            final String dataPlatformSnapshotStr = "com.linkedin.pegasus2avro.metadata.snapshot.DataPlatformSnapshot";
            final String dataPlatformInfoStr = "com.linkedin.pegasus2avro.dataplatform.DataPlatformInfo";
            final Urn urn = Urn.createFromString(dataPlatformObj.get("proposedSnapshot").get(dataPlatformSnapshotStr).get("urn").asText());
            final DataPlatformInfo info = RecordUtils.toRecordTemplate(DataPlatformInfo.class,
                    dataPlatformObj.get("proposedSnapshot").get(dataPlatformSnapshotStr).get("aspects").get(0).get(dataPlatformInfoStr).toString()
            );
            // 3. Write key & aspect
            final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
            final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
            GenericAspect aspect = GenericAspectUtils.serializeAspect(
                    EntityKeyUtils.convertUrnToEntityKey(
                            urn, keyAspectSpec.getPegasusSchema()));
            keyAspectProposal.setAspect(aspect);
            keyAspectProposal.setAspectName(keyAspectSpec.getName());
            keyAspectProposal.setEntityType(PLATFORM_ENTITY_NAME);
            keyAspectProposal.setChangeType(ChangeType.UPSERT);
            keyAspectProposal.setEntityUrn(urn);

            _entityService.ingestProposal(keyAspectProposal, new AuditStamp()
                    .setActor(Urn.createFromString("urn:li:dataPlatform"))
                    .setTime(System.currentTimeMillis()));

            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityUrn(urn);
            proposal.setEntityType(PLATFORM_ENTITY_NAME);
            proposal.setAspectName(PLATFORM_ASPECT_NAME);
            proposal.setAspect(GenericAspectUtils.serializeAspect(info));
            proposal.setChangeType(ChangeType.UPSERT);

            _entityService.ingestProposal(proposal, new AuditStamp()
                    .setActor(Urn.createFromString("urn:li:dataPlatform"))
                    .setTime(System.currentTimeMillis()));
        }
    }
}
