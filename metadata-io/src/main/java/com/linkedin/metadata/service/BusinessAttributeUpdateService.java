package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.common.AuditStamp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

@Slf4j
@Component
public class BusinessAttributeUpdateService {
    private static final String EDITABLE_SCHEMAFIELD_WITH_BUSINESS_ATTRIBUTE = "EditableSchemaFieldWithBusinessAttribute";

    private final GraphService _graphService;
    private final EntityService _entityService;
    private final EntityRegistry _entityRegistry;

    public static final String TAG = "TAG";
    public static final String GLOSSARY_TERM = "GLOSSARY_TERM";
    public static final String DOCUMENTATION = "DOCUMENTATION";

    public BusinessAttributeUpdateService(GraphService graphService, EntityService entityService,
                                          EntityRegistry entityRegistry) {
        this._graphService = graphService;
        this._entityService = entityService;
        this._entityRegistry = entityRegistry;
    }

    public void handleChangeEvent(@Nonnull final PlatformEvent event) {
        final EntityChangeEvent entityChangeEvent =
                GenericRecordUtils.deserializePayload(event.getPayload().getValue(), EntityChangeEvent.class);

        if (!entityChangeEvent.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
            log.info("Skipping MCL event for invalid event entity type: " + entityChangeEvent.getEntityType());
            return;
        }

        final Set<String> businessAttributeCategories =
                ImmutableSet.of(TAG, GLOSSARY_TERM, DOCUMENTATION);
        if (!businessAttributeCategories.contains(entityChangeEvent.getCategory())) {
            log.info("Skipping MCL event for invalid event category: " + entityChangeEvent.getCategory());
            return;
        }

        Urn urn = entityChangeEvent.getEntityUrn();
        log.info("Business Attribute update hook invoked for :" + urn.toString());

        RelatedEntitiesResult relatedEntitiesResult = _graphService.findRelatedEntities(
                null, newFilter("urn", urn.toString()), null,
                EMPTY_FILTER, Arrays.asList(EDITABLE_SCHEMAFIELD_WITH_BUSINESS_ATTRIBUTE),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 100000);

        for (RelatedEntity relatedEntity : relatedEntitiesResult.getEntities()) {
            String datasetUrnStr = relatedEntity.getUrn();
            Map<Urn, EntityResponse> datasetEntityResponses;
            try {
                Urn datasetUrn = new Urn(datasetUrnStr);
                final AspectSpec datasetAspectSpec = _entityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME)
                        .getAspectSpec(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
                datasetEntityResponses = _entityService.getEntitiesV2(Constants.DATASET_ENTITY_NAME,
                        new HashSet<>(Arrays.asList(datasetUrn)),
                        Collections.singleton(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)
                );

                EntityResponse datasetEntityResponse = datasetEntityResponses.get(datasetUrn);
                EditableDatasetProperties datasetProperties = mapTermInfo(datasetEntityResponse);
                final AuditStamp auditStamp =
                        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

                _entityService.alwaysProduceMCLAsync(
                        datasetUrn,
                        Constants.DATASET_ENTITY_NAME,
                        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                        datasetAspectSpec,
                        null,
                        datasetProperties,
                        null,
                        null,
                        auditStamp,
                        ChangeType.RESTATE).getFirst();

            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private EditableDatasetProperties mapTermInfo(EntityResponse entityResponse) {
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        if (!aspectMap.containsKey(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)) {
            return null;
        }
        return new EditableDatasetProperties(aspectMap.get(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    }
}
