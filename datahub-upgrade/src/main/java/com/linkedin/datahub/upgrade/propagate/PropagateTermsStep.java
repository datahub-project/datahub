package com.linkedin.datahub.upgrade.propagate;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.propagate.comparator.EntityMatcher;
import com.linkedin.datahub.upgrade.propagate.comparator.SchemaBasedMatcher;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;


@Slf4j
@RequiredArgsConstructor
public class PropagateTermsStep implements UpgradeStep {

  private final EntityService _entityService;
  private final EntitySearchService _entitySearchService;
  private final EntityMatcher _entityMatcher;
  private final EntityFetcher _entityFetcher;

  private static final Urn PROPAGATION_ACTOR = UrnUtils.getUrn("urn:li:corpuser:__datahub_propagator");

  public PropagateTermsStep(EntityService entityService, EntitySearchService entitySearchService) {
    _entityService = entityService;
    _entitySearchService = entitySearchService;
    _entityMatcher = new SchemaBasedMatcher();

    _entityFetcher = new EntityFetcher(entityService);
  }

  @Override
  public String id() {
    return "PropagateTermsStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      String runId = createRunId();

      context.report().addLine(String.format("Starting term propagation (Run ID: %s)...", runId));

//      Optional<String> sourceFilter = context.parsedArgs().get("SOURCE_FILTER");
//      if (!sourceFilter.isPresent()) {
//        context.report().addLine("Missing required arguments. This job requires SOURCE_FILTER");
//        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
//      }
      Filter sourceFilter = QueryUtils.newFilter("platform.keyword", "urn:li:dataPlatform:postgres");

      context.report().addLine("Fetching source entities to propagate from");

      SearchResult sourceSearchResults =
          _entitySearchService.filter(Constants.DATASET_ENTITY_NAME, sourceFilter, null, 0, 5000);

      context.report().addLine(String.format("Found %d source entities", sourceSearchResults.getNumEntities()));
      context.report().addLine("Fetching schema for the source entities");

      Map<Urn, EntityDetails> sourceEntityDetails = _entityFetcher.fetchSchema(
          sourceSearchResults.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toSet()))
          .entrySet()
          .stream()
          .filter(entry -> validSource(entry.getValue()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      context.report().addLine("Fetching all other entities");

      int batch = 1;
      context.report().addLine(String.format("Fetching batch %d", batch));
      ScrollResult scrollResult =
          _entitySearchService.scroll(Constants.DATASET_ENTITY_NAME, null, null, 1000, null, "1m");
      while (scrollResult.getEntities().size() > 0) {
        context.report().addLine(String.format("Processing batch %d", batch));
        processBatch(scrollResult, sourceEntityDetails, runId);
        batch++;
        context.report().addLine(String.format("Fetching batch %d", batch));
        scrollResult = _entitySearchService.scroll(Constants.DATASET_ENTITY_NAME, null, null, 1000,
            scrollResult.getScrollId(), "1m");
      }

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private String createRunId() {
    return "term_propagation_" + System.currentTimeMillis() + "_" + RandomStringUtils.randomAlphabetic(4);
  }

  private boolean validSource(EntityDetails entityDetails) {
    if (entityDetails.getSchemaMetadata() == null || entityDetails.getSchemaMetadata().getFields().isEmpty()) {
      return false;
    }
    boolean hasTerms = entityDetails.getSchemaMetadata()
        .getFields()
        .stream()
        .filter(SchemaField::hasGlossaryTerms)
        .flatMap(field -> field.getGlossaryTerms().getTerms().stream().map(GlossaryTermAssociation::getUrn))
        .anyMatch(e -> true);
    if (hasTerms) {
      return true;
    }
    if (entityDetails.getEditableSchemaMetadata() == null || entityDetails.getEditableSchemaMetadata()
        .getEditableSchemaFieldInfo()
        .isEmpty()) {
      return false;
    }
    return entityDetails.getEditableSchemaMetadata()
        .getEditableSchemaFieldInfo()
        .stream()
        .filter(EditableSchemaFieldInfo::hasGlossaryTerms)
        .flatMap(field -> field.getGlossaryTerms().getTerms().stream().map(GlossaryTermAssociation::getUrn))
        .anyMatch(e -> true);
  }

  private void processBatch(@Nonnull ScrollResult scrollResult, @Nonnull Map<Urn, EntityDetails> sourceEntityDetails,
      @Nonnull String runId) {
    Set<Urn> batch = scrollResult.getEntities()
        .stream()
        .map(SearchEntity::getEntity)
        .filter(entity -> !sourceEntityDetails.containsKey(entity))
        .collect(Collectors.toSet());
    log.info("Fetching schema for batch of {} urns", batch.size());

    Map<Urn, EntityDetails> entityDetails = _entityFetcher.fetchSchema(batch);
    for (Urn destUrn : entityDetails.keySet()) {
      EntityDetails destinationEntity = entityDetails.get(destUrn);
      EntityMatcher.EntityMatchResult matchResult =
          _entityMatcher.match(destinationEntity, sourceEntityDetails.values());
      if (matchResult == null) {
        continue;
      }
      processMatch(destinationEntity, matchResult, runId);
    }
  }

  private void processMatch(@Nonnull EntityDetails destinationEntity,
      @Nonnull EntityMatcher.EntityMatchResult entityMatchResult, @Nonnull String runId) {
    log.info("Processing match for source {} destination {}", entityMatchResult.getMatchedEntity().getUrn(),
        destinationEntity.getUrn());
    if (entityMatchResult.getMatchingFields().isEmpty()) {
      log.info("No matching fields for source {} destination {}", entityMatchResult.getMatchedEntity().getUrn(),
          destinationEntity.getUrn());
      return;
    }

    Map<String, Set<Urn>> sourceFieldGlossaryTerms = getGlossaryTermsForEachField(entityMatchResult.getMatchedEntity());
    Map<String, Set<Urn>> destinationFieldGlossaryTerms = getGlossaryTermsForEachField(destinationEntity);
    sourceFieldGlossaryTerms.forEach((fieldPath, glossaryTermsToPropagate) -> {
      if (destinationFieldGlossaryTerms.containsKey(fieldPath)) {
        glossaryTermsToPropagate.removeAll(destinationFieldGlossaryTerms.get(fieldPath));
      }
    });
    sourceFieldGlossaryTerms.values().removeIf(Set::isEmpty);

    if (sourceFieldGlossaryTerms.isEmpty()) {
      log.info("No terms to propagate for source {} destination {}", entityMatchResult.getMatchedEntity().getUrn(),
          destinationEntity.getUrn());
      return;
    }

    EditableSchemaMetadata aspectToPush =
        buildSchemaMetadata(destinationEntity.getEditableSchemaMetadata(), sourceFieldGlossaryTerms);
    produceEditableSchemaMetadataProposal(destinationEntity.getUrn(), aspectToPush, runId);
  }

  private Map<String, Set<Urn>> getGlossaryTermsForEachField(EntityDetails entityDetails) {
    Map<String, Set<Urn>> result = new HashMap<>();
    if (entityDetails.getSchemaMetadata() == null) {
      return result;
    }
    for (SchemaField field : entityDetails.getSchemaMetadata().getFields()) {
      if (field.hasGlossaryTerms() && !field.getGlossaryTerms().getTerms().isEmpty()) {
        result.put(field.getFieldPath(), field.getGlossaryTerms()
            .getTerms()
            .stream()
            .map(GlossaryTermAssociation::getUrn)
            .collect(Collectors.toSet()));
      }
    }
    if (entityDetails.getEditableSchemaMetadata() == null) {
      return result;
    }
    for (EditableSchemaFieldInfo field : entityDetails.getEditableSchemaMetadata().getEditableSchemaFieldInfo()) {
      if (field.hasGlossaryTerms() && !field.getGlossaryTerms().getTerms().isEmpty()) {
        if (result.containsKey(field.getFieldPath())) {
          result.put(field.getFieldPath(), field.getGlossaryTerms()
              .getTerms()
              .stream()
              .map(GlossaryTermAssociation::getUrn)
              .collect(Collectors.toSet()));
        } else {
          Set<Urn> termsSoFar = result.get(field.getFieldPath());
          field.getGlossaryTerms().getTerms().stream().map(GlossaryTermAssociation::getUrn).forEach(termsSoFar::add);
        }
      }
    }
    return result;
  }

  private EditableSchemaMetadata buildSchemaMetadata(@Nullable EditableSchemaMetadata oldAspect,
      @Nonnull Map<String, Set<Urn>> termsToAddPerField) {
    List<EditableSchemaFieldInfo> resultFields = new ArrayList<>();
    List<EditableSchemaFieldInfo> originalFields =
        oldAspect != null ? oldAspect.getEditableSchemaFieldInfo() : Collections.emptyList();
    // Modify existing schema fields
    for (EditableSchemaFieldInfo field : originalFields) {
      if (termsToAddPerField.containsKey(field.getFieldPath())) {
        Set<Urn> termsToAdd = termsToAddPerField.get(field.getFieldPath());
        GlossaryTerms glossaryTerms = field.getGlossaryTerms();
        if (glossaryTerms == null) {
          field.setGlossaryTerms(buildGlossaryTerms(termsToAdd));
        } else {
          termsToAdd.forEach(term -> glossaryTerms.getTerms().add(buildGlossaryTermAssociation(term)));
        }
        termsToAddPerField.remove(field.getFieldPath());
      }
    }

    // Add remaining fields
    for (String fieldPath : termsToAddPerField.keySet()) {
      resultFields.add(buildSchemaFieldInfo(fieldPath, termsToAddPerField.get(fieldPath)));
    }

    return new EditableSchemaMetadata().setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray(resultFields));
  }

  private EditableSchemaFieldInfo buildSchemaFieldInfo(@Nonnull String fieldPath, @Nonnull Set<Urn> terms) {
    return new EditableSchemaFieldInfo().setFieldPath(fieldPath).setGlossaryTerms(buildGlossaryTerms(terms));
  }

  private GlossaryTerms buildGlossaryTerms(@Nonnull Set<Urn> terms) {
    GlossaryTermAssociationArray termAssociations = new GlossaryTermAssociationArray();
    terms.forEach(term -> termAssociations.add(buildGlossaryTermAssociation(term)));
    return new GlossaryTerms().setTerms(termAssociations);
  }

  @SneakyThrows
  private GlossaryTermAssociation buildGlossaryTermAssociation(Urn termUrn) {
    return new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(termUrn)).setActor(PROPAGATION_ACTOR);
  }

  private void produceEditableSchemaMetadataProposal(@Nonnull Urn urn,
      @Nonnull EditableSchemaMetadata editableSchemaMetadata, @Nonnull String runId) {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableSchemaMetadata));
    proposal.setChangeType(ChangeType.UPSERT);

    SystemMetadata systemMetadata = new SystemMetadata().setRunId(runId);
    proposal.setSystemMetadata(systemMetadata);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(PROPAGATION_ACTOR).setTime(System.currentTimeMillis()));
  }
}
