package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MigrateDataProcessInstanceEdgesStep implements UpgradeStep {
  private static final String UPGRADE_ID =
      MigrateDataProcessInstanceEdgesStep.class.getSimpleName();
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final boolean reprocessEnabled;
  private final List<String> inputPlatforms;
  private final List<String> outputPlatforms;
  private final List<String> parentPlatforms;
  private final Integer batchSize;

  public MigrateDataProcessInstanceEdgesStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      List<String> inputPlatforms,
      List<String> outputPlatforms,
      List<String> parentPlatforms,
      Integer batchSize) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.reprocessEnabled = reprocessEnabled;
    this.inputPlatforms = inputPlatforms;
    this.outputPlatforms = outputPlatforms;
    this.parentPlatforms = parentPlatforms;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
      final Filter filter = dataProcessInstancesInputOutputFilter();

      String scrollId = null;
      int migratedCount = 0;
      do {
        log.info(
            String.format(
                "Migrating batch %s-%s of data process instance edges",
                migratedCount, migratedCount + batchSize));
        scrollId = migrateBatch(filter, auditStamp, scrollId);
        migratedCount += batchSize;
      } while (scrollId != null);

      BootstrapStep.setUpgradeResult(systemOpContext, UPGRADE_ID_URN, entityService);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private String migrateBatch(Filter filter, AuditStamp auditStamp, String scrollId) {
    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            systemOpContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            List.of(DATA_PROCESS_INSTANCE_ENTITY_NAME),
            "*",
            filter,
            null,
            scrollId,
            null,
            batchSize,
            null);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
      return null;
    }

    try {
      migrateUrns(scrollResult.getEntities(), auditStamp);
    } catch (Exception e) {
      // don't stop the whole step because of one bad urn or one bad ingestion
      log.error(
          String.format(
              "Error ingesting ownership aspect for urn %s",
              scrollResult.getEntities().stream()
                  .map(SearchEntity::getEntity)
                  .collect(Collectors.toList())),
          e);
    }

    return scrollResult.getScrollId();
  }

  private void migrateUrns(SearchEntityArray searchBatch, AuditStamp auditStamp) {
    Map<Urn, Map<String, Aspect>> existing =
        systemOpContext
            .getRetrieverContext()
            .getAspectRetriever()
            .getLatestAspectObjects(
                searchBatch.stream().map(SearchEntity::getEntity).collect(Collectors.toSet()),
                Set.of(
                    DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME,
                    DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME));

    Stream<MetadataChangeProposal> inputMcps =
        existing.entrySet().stream()
            .filter(
                result -> result.getValue().containsKey(DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME))
            .map(
                result -> {
                  Aspect aspect = result.getValue().get(DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME);
                  DataProcessInstanceInput dpiInput =
                      RecordUtils.toRecordTemplate(DataProcessInstanceInput.class, aspect.data());
                  dpiInput.setInputEdges(
                      new EdgeArray(
                          dpiInput.getInputs().stream()
                              .map(urn -> new Edge().setDestinationUrn(urn))
                              .collect(Collectors.toList())));

                  MetadataChangeProposal proposal = new MetadataChangeProposal();
                  proposal.setEntityUrn(result.getKey());
                  proposal.setEntityType(result.getKey().getEntityType());
                  proposal.setAspectName(Constants.DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME);
                  proposal.setChangeType(ChangeType.UPSERT);
                  proposal.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
                  proposal.setAspect(GenericRecordUtils.serializeAspect(dpiInput));
                  return proposal;
                });

    Stream<MetadataChangeProposal> outputMcps =
        existing.entrySet().stream()
            .filter(
                result -> result.getValue().containsKey(DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME))
            .map(
                result -> {
                  Aspect aspect = result.getValue().get(DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME);
                  DataProcessInstanceOutput dpiOutput =
                      RecordUtils.toRecordTemplate(DataProcessInstanceOutput.class, aspect.data());
                  dpiOutput.setOutputEdges(
                      new EdgeArray(
                          dpiOutput.getOutputs().stream()
                              .map(urn -> new Edge().setDestinationUrn(urn))
                              .collect(Collectors.toList())));

                  MetadataChangeProposal proposal = new MetadataChangeProposal();
                  proposal.setEntityUrn(result.getKey());
                  proposal.setEntityType(result.getKey().getEntityType());
                  proposal.setAspectName(Constants.DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME);
                  proposal.setChangeType(ChangeType.UPSERT);
                  proposal.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
                  proposal.setAspect(GenericRecordUtils.serializeAspect(dpiOutput));
                  return proposal;
                });

    List<MetadataChangeProposal> mcps =
        Stream.concat(inputMcps, outputMcps).collect(Collectors.toList());
    log.debug(String.format("Migrating data process instance edges: %d aspects", mcps.size()));
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(mcps, auditStamp, systemOpContext.getRetrieverContext())
            .build(systemOpContext);

    entityService.ingestProposal(systemOpContext, batch, false);
  }

  @VisibleForTesting
  protected Filter dataProcessInstancesInputOutputFilter() {
    Criterion inputPlatform =
        CriterionUtils.buildCriterion("inputs", Condition.CONTAIN, false, this.inputPlatforms);
    Criterion outputPlatform =
        CriterionUtils.buildCriterion("outputs", Condition.CONTAIN, false, this.outputPlatforms);
    Criterion parentPlatform =
        CriterionUtils.buildCriterion("parent", Condition.CONTAIN, false, this.parentPlatforms);

    CriterionArray inputCriterionArray = new CriterionArray();
    inputCriterionArray.add(inputPlatform);
    CriterionArray outputCriterionArray = new CriterionArray();
    outputCriterionArray.add(outputPlatform);
    CriterionArray parentCriterionArray = new CriterionArray();
    parentCriterionArray.add(parentPlatform);

    ConjunctiveCriterion inputAnds = new ConjunctiveCriterion();
    inputAnds.setAnd(inputCriterionArray);
    ConjunctiveCriterion outputAnds = new ConjunctiveCriterion();
    outputAnds.setAnd(outputCriterionArray);
    ConjunctiveCriterion parentAnds = new ConjunctiveCriterion();
    parentAnds.setAnd(parentCriterionArray);

    ConjunctiveCriterionArray ors = new ConjunctiveCriterionArray();
    ors.add(inputAnds);
    ors.add(outputAnds);
    ors.add(parentAnds);

    Filter filter = new Filter();
    filter.setOr(ors);
    return filter;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }
}
