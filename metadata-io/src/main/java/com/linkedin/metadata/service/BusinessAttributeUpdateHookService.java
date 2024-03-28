package com.linkedin.metadata.service;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.google.common.collect.ImmutableSet;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
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
import java.util.Arrays;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BusinessAttributeUpdateHookService {
  private static final String BUSINESS_ATTRIBUTE_OF = "BusinessAttributeOf";

  private final GraphService graphService;
  private final EntityService entityService;
  private final EntityRegistry entityRegistry;

  private final int relatedEntitiesCount;

  public static final String TAG = "TAG";
  public static final String GLOSSARY_TERM = "GLOSSARY_TERM";
  public static final String DOCUMENTATION = "DOCUMENTATION";

  public BusinessAttributeUpdateHookService(
      GraphService graphService,
      EntityService entityService,
      EntityRegistry entityRegistry,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesCount}") int relatedEntitiesCount) {
    this.graphService = graphService;
    this.entityService = entityService;
    this.entityRegistry = entityRegistry;
    this.relatedEntitiesCount = relatedEntitiesCount;
  }

  public void handleChangeEvent(@NonNull final PlatformEvent event) {
    final EntityChangeEvent entityChangeEvent =
        GenericRecordUtils.deserializePayload(
            event.getPayload().getValue(), EntityChangeEvent.class);

    if (!entityChangeEvent.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
      log.info("Skipping MCL event for entity:" + entityChangeEvent.getEntityType());
      return;
    }

    final Set<String> businessAttributeCategories =
        ImmutableSet.of(TAG, GLOSSARY_TERM, DOCUMENTATION);
    if (!businessAttributeCategories.contains(entityChangeEvent.getCategory())) {
      log.info("Skipping MCL event for category: " + entityChangeEvent.getCategory());
      return;
    }

    Urn urn = entityChangeEvent.getEntityUrn();
    log.info("Business Attribute update hook invoked for urn :" + urn);

    RelatedEntitiesResult entityAssociatedWithBusinessAttribute =
        graphService.findRelatedEntities(
            null,
            newFilter("urn", urn.toString()),
            null,
            EMPTY_FILTER,
            Arrays.asList(BUSINESS_ATTRIBUTE_OF),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            0,
            relatedEntitiesCount);

    for (RelatedEntity relatedEntity : entityAssociatedWithBusinessAttribute.getEntities()) {
      String entityUrnStr = relatedEntity.getUrn();
      try {
        Urn entityUrn = new Urn(entityUrnStr);
        final AspectSpec aspectSpec =
            entityRegistry
                .getEntitySpec(Constants.SCHEMA_FIELD_ENTITY_NAME)
                .getAspectSpec(Constants.BUSINESS_ATTRIBUTE_ASPECT);

        EnvelopedAspect envelopedAspect =
            entityService.getLatestEnvelopedAspect(
                Constants.SCHEMA_FIELD_ENTITY_NAME, entityUrn, Constants.BUSINESS_ATTRIBUTE_ASPECT);
        BusinessAttributes businessAttributes =
            new BusinessAttributes(envelopedAspect.getValue().data());

        final AuditStamp auditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());

        entityService
            .alwaysProduceMCLAsync(
                entityUrn,
                Constants.SCHEMA_FIELD_ENTITY_NAME,
                Constants.BUSINESS_ATTRIBUTE_ASPECT,
                aspectSpec,
                null,
                businessAttributes,
                null,
                null,
                auditStamp,
                ChangeType.RESTATE)
            .getFirst();

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
