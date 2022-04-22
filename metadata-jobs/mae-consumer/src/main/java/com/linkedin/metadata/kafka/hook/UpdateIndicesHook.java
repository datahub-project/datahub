package com.linkedin.metadata.kafka.hook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.search.utils.QueryUtils.*;

@Slf4j
@Component
@Import({EntityRegistryFactory.class, GraphServiceFactory.class, EntitySearchServiceFactory.class,
    TimeseriesAspectServiceFactory.class, EntityRegistryFactory.class, SystemMetadataServiceFactory.class,
    SearchDocumentTransformerFactory.class})
public class UpdateIndicesHook implements MetadataChangeLogHook {

  private final EntityService _entityService;
  private final GraphService _graphService;
  private final EntitySearchService _entitySearchService;
  private final TimeseriesAspectService _timeseriesAspectService;
  private final SystemMetadataService _systemMetadataService;
  private final EntityRegistry _entityRegistry;
  private final SearchDocumentTransformer _searchDocumentTransformer;

  @Autowired
  public UpdateIndicesHook(
      EntityService entityService,
      GraphService graphService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      EntityRegistry entityRegistry,
      SearchDocumentTransformer searchDocumentTransformer) {
    _entityService = entityService;
    _graphService = graphService;
    _entitySearchService = entitySearchService;
    _timeseriesAspectService = timeseriesAspectService;
    _systemMetadataService = systemMetadataService;
    _entityRegistry = entityRegistry;
    _searchDocumentTransformer = searchDocumentTransformer;
    _graphService.configure();
    _entitySearchService.configure();
    _systemMetadataService.configure();
    _timeseriesAspectService.configure();
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      return;
    }
    Urn urn = EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());

    if (event.getChangeType() == ChangeType.UPSERT) {

      if (!event.hasAspectName() || !event.hasAspect()) {
        log.error("Aspect or aspect name is missing");
        return;
      }

      AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
      if (aspectSpec == null) {
        log.error("Unrecognized aspect name {} for entity {}", event.getAspectName(), event.getEntityType());
        return;
      }

      RecordTemplate aspect =
          GenericRecordUtils.deserializeAspect(event.getAspect().getValue(), event.getAspect().getContentType(),
              aspectSpec);
      if (aspectSpec.isTimeseries()) {
        updateTimeseriesFields(event.getEntityType(), event.getAspectName(), urn, aspect, aspectSpec,
            event.getSystemMetadata());
      } else {
        updateSearchService(entitySpec.getName(), urn, aspectSpec, aspect);
        updateGraphService(urn, aspectSpec, aspect);
        updateSystemMetadata(event.getSystemMetadata(), urn, aspectSpec, aspect);
      }
    } else if (event.getChangeType() == ChangeType.DELETE) {
      if (!event.hasAspectName() || !event.hasPreviousAspectValue()) {
        log.error("Previous aspect or aspect name is missing");
        return;
      }

      AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
      if (aspectSpec == null) {
        log.error("Unrecognized aspect name {} for entity {}", event.getAspectName(), event.getEntityType());
        return;
      }

      RecordTemplate aspect = GenericRecordUtils.deserializeAspect(event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(), aspectSpec);
      Boolean isDeletingKey = event.getAspectName().equals(entitySpec.getKeyAspectName());

      if (!aspectSpec.isTimeseries()) {
        deleteSystemMetadata(urn, aspectSpec, isDeletingKey);
        deleteGraphData(urn, aspectSpec, aspect, isDeletingKey);
        deleteSearchData(urn, entitySpec.getName(), aspectSpec, aspect, isDeletingKey);
      }
    }
  }

  private Pair<List<Edge>, Set<String>> getEdgesAndRelationshipTypesFromAspect(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    final Set<String> relationshipTypesBeingAdded = new HashSet<>();
    final List<Edge> edgesToAdd = new ArrayList<>();

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getRelationshipFieldSpecs());

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      relationshipTypesBeingAdded.add(entry.getKey().getRelationshipName());
      for (Object fieldValue : entry.getValue()) {
        try {
          edgesToAdd.add(
              new Edge(urn, Urn.createFromString(fieldValue.toString()), entry.getKey().getRelationshipName()));
        } catch (URISyntaxException e) {
          log.error("Invalid destination urn: {}", fieldValue.toString(), e);
        }
      }
    }
    return Pair.of(edgesToAdd, relationshipTypesBeingAdded);
  }

  /**
   * Process snapshot and update graph index
   */
  private void updateGraphService(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    Pair<List<Edge>, Set<String>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, aspect);

    final List<Edge> edgesToAdd = edgeAndRelationTypes.getFirst();
    final Set<String> relationshipTypesBeingAdded = edgeAndRelationTypes.getSecond();

    log.debug("Here's the relationship types found {}", relationshipTypesBeingAdded);
    if (relationshipTypesBeingAdded.size() > 0) {
      _graphService.removeEdgesFromNode(urn, new ArrayList<>(relationshipTypesBeingAdded),
          newRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING));
      edgesToAdd.forEach(edge -> _graphService.addEdge(edge));
    }
  }

  /**
   * Process snapshot and update search index
   */
  private void updateSearchService(String entityName, Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    Optional<String> searchDocument;
    try {
      searchDocument = _searchDocumentTransformer.transformAspect(urn, aspect, aspectSpec, false);
    } catch (Exception e) {
      log.error("Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    Optional<String> docId = SearchUtils.getDocId(urn);

    if (!docId.isPresent()) {
      return;
    }

    _entitySearchService.upsertDocument(entityName, searchDocument.get(), docId.get());
  }

  /**
   * Process snapshot and update timseries index
   */
  private void updateTimeseriesFields(String entityType, String aspectName, Urn urn, RecordTemplate aspect,
      AspectSpec aspectSpec, SystemMetadata systemMetadata) {
    Map<String, JsonNode> documents;
    try {
      documents = TimeseriesAspectTransformer.transform(urn, aspect, aspectSpec, systemMetadata);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
      return;
    }
    documents.entrySet().forEach(document -> {
      _timeseriesAspectService.upsertDocument(entityType, aspectName, document.getKey(), document.getValue());
    });
  }

  private void updateSystemMetadata(SystemMetadata systemMetadata, Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    _systemMetadataService.insert(systemMetadata, urn.toString(), aspectSpec.getName());

    // If processing status aspect update all aspects for this urn to removed
    if (aspectSpec.getName().equals(Constants.STATUS_ASPECT_NAME)) {
      _systemMetadataService.setDocStatus(urn.toString(), ((Status) aspect).isRemoved());
    }
  }

  private void deleteSystemMetadata(Urn urn, AspectSpec aspectSpec, Boolean isKeyAspect) {
    if (isKeyAspect) {
      // Delete all aspects
      log.debug(String.format("Deleting all system metadata for urn: %s", urn));
      _systemMetadataService.deleteUrn(urn.toString());
    } else {
      // Delete all aspects from system metadata service
      log.debug(String.format("Deleting system metadata for urn: %s, aspect: %s", urn, aspectSpec.getName()));
      _systemMetadataService.deleteAspect(urn.toString(), aspectSpec.getName());
    }
  }

  private void deleteGraphData(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect, Boolean isKeyAspect) {
    if (isKeyAspect) {
      removeGraphReferencesTo(urn);
      _graphService.removeNode(urn);
      return;
    }

    Pair<List<Edge>, Set<String>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, aspect);

    final Set<String> relationshipTypesBeingAdded = edgeAndRelationTypes.getSecond();
    if (relationshipTypesBeingAdded.size() > 0) {
      _graphService.removeEdgesFromNode(urn, new ArrayList<>(relationshipTypesBeingAdded),
          createRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING));
    }
  }

  private void removeGraphReferencesTo(final Urn urn) {
    // Process all related entities affected by the urn being deleted, in a paginated fashion.
    int offset = 0;
    int relatedAspectsUpdated = 0;
    do {

      final RelatedEntitiesResult relatedEntities =
          _graphService.findRelatedEntities(null, newFilter("urn", urn.toString()), null,
              EMPTY_FILTER,
              ImmutableList.of(),
              newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), offset, 100);

      for (RelatedEntity relatedEntity : relatedEntities.getEntities()) {

        try {

          final Urn relatedUrn = UrnUtils.getUrn(relatedEntity.getUrn());
          final String relationType = relatedEntity.getRelationshipType();
          final String relatedEntityName = relatedUrn.getEntityType();

          log.info(String.format("Processing related entity %s with relationship %s", relatedEntityName,
              relationType));

          final EntitySpec relatedEntitySpec = _entityRegistry.getEntitySpec(relatedEntityName);

          final java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> optionalAspectInfo =
              findAspectDetails(urn, relatedUrn, relationType, relatedEntitySpec);

          if (!optionalAspectInfo.isPresent()) {
            log.error(String.format("Unable to find aspect information that relates %s %s via relationship %s",
                urn, relatedUrn, relationType));
            continue;
          }

          final String aspectName = optionalAspectInfo.get().getKey().getName();
          final Aspect aspect = optionalAspectInfo.get().getKey().getValue();
          final AspectSpec aspectSpec = optionalAspectInfo.get().getValue();
          final java.util.Optional<RelationshipFieldSpec> optionalRelationshipFieldSpec =
              aspectSpec.findRelationshipFor(relationType, urn.getEntityType());

          if (!optionalRelationshipFieldSpec.isPresent()) {
            log.error(String.format("Unable to find relationship spec information in %s that connects to %s via %s",
                aspectName, urn.getEntityType(), relationType));
            continue;
          }

          final PathSpec path = optionalRelationshipFieldSpec.get().getPath();
          final Aspect updatedAspect = AspectProcessor.removeAspect(urn.toString(), aspect,
              aspectSpec.getPegasusSchema(), path);

          final MetadataChangeProposal gmce = new MetadataChangeProposal();
          gmce.setEntityUrn(relatedUrn);
          gmce.setChangeType(ChangeType.UPSERT);
          gmce.setEntityType(relatedEntityName);
          gmce.setAspectName(aspectName);
          gmce.setAspect(GenericRecordUtils.serializeAspect(updatedAspect));
          final AuditStamp auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

          final EntityService.IngestProposalResult ingestProposalResult = _entityService.ingestProposal(gmce, auditStamp);

          if (!ingestProposalResult.isDidUpdate()) {
            log.warn(String.format("Aspect update did not update metadata graph. Before %s, after: %s",
                aspect, updatedAspect));
          }
          relatedAspectsUpdated++;

        } catch (CloneNotSupportedException e) {
          log.error(String.format("Failed to clone aspect from entity %s", relatedEntity), e);
        } catch (URISyntaxException e) {
          log.error(String.format("Failed to process related entity %s", relatedEntity), e);
        }
      }
      offset = relatedEntities.getEntities().size();
    } while (offset > 0);
  }

  /**
   * Utility method that attempts to find Aspect information as well as the associated path spec for a given urn that
   * has a relationship of type `relationType` to another urn
   * @param urn               The urn of the entity for which we want to find the aspect that relates the urn to
   *                          `relatedUrn`.
   * @param relatedUrn        The urn of the related entity in which we want to find the aspect that has a relationship
   *                          to `urn`.
   * @param relationType      The relationship type that details how the `urn` entity is related to `relatedUrn` entity.
   * @param relatedEntitySpec The entity spec of the related entity.
   * @return An {@link java.util.Optional} object containing the aspect content & respective spec that contains the
   * relationship between `urn` & `relatedUrn`.
   * @throws URISyntaxException
   */
  private java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> findAspectDetails(Urn urn, Urn relatedUrn,
      String relationType, EntitySpec relatedEntitySpec) throws URISyntaxException {

    // Find which aspects are the candidates for the relationship we are looking for
    final Map<String, AspectSpec> aspectSpecs = Objects.requireNonNull(relatedEntitySpec).getAspectSpecMap()
        .entrySet()
        .stream()
        .filter(aspectSpec -> aspectSpec.getValue().getRelationshipFieldSpecs()
            .stream()
            .anyMatch(spec -> spec.getRelationshipName().equals(relationType)
                && spec.getValidDestinationTypes().contains(urn.getEntityType())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (aspectSpecs.size() > 1) {
      log.warn(String.format("More than 1 relationship of type %s expected for destination entity: %s", relationType,
          urn.getEntityType()));
    }

    final EntityResponse entityResponse = _entityService.getEntityV2(relatedUrn.getEntityType(),
        relatedUrn,
        aspectSpecs.keySet());

    // Find aspect which contains the relationship with the value we are looking for
    return Objects.requireNonNull(entityResponse)
        .getAspects().values()
        .stream()
        .filter(aspect -> aspect.getValue().toString().contains(urn.getEntityType()))
        .map(aspect -> Pair.of(aspect, aspectSpecs.get(aspect.getName())))
        .findFirst();
  }

  private void deleteSearchData(Urn urn, String entityName, AspectSpec aspectSpec, RecordTemplate aspect, Boolean isKeyAspect) {
      String docId;
      try {
        docId = URLEncoder.encode(urn.toString(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        log.error("Failed to encode the urn with error: {}", e.toString());
        return;
      }

      if (isKeyAspect) {
        _entitySearchService.deleteDocument(entityName, docId);
        return;
      }

      Optional<String> searchDocument;
      try {
        searchDocument = _searchDocumentTransformer.transformAspect(urn, aspect, aspectSpec, true);
      } catch (Exception e) {
        log.error("Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
        return;
      }

      if (!searchDocument.isPresent()) {
        return;
      }

    _entitySearchService.upsertDocument(entityName, searchDocument.get(), docId);
  }
}