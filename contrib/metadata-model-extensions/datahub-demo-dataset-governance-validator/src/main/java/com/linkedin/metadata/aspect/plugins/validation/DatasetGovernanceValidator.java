package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom validator that ensures logical dataset URNs have ownership, tags, and domain set either in
 * the current batch of MCPs or already existing in GMS.
 *
 * <p>This validator checks batches of MCPs to ensure that any logical dataset entities (those with
 * dataPlatform:logical) have the required governance metadata (ownership, tags, domain) either: 1.
 * Already present in the system (retrieved via AspectRetriever) 2. Being set in the current batch
 * of changes
 *
 * <p>The validator operates at the batch level to efficiently validate multiple related changes
 * together. Only datasets with urn:li:dataPlatform:logical are validated.
 */
public class DatasetGovernanceValidator extends AspectPayloadValidator {

  private static final Logger logger = LoggerFactory.getLogger(DatasetGovernanceValidator.class);

  private static final String DATASET_ENTITY_TYPE = "dataset";
  private static final String LOGICAL_DATA_PLATFORM = "urn:li:dataPlatform:logical";
  private static final Set<String> REQUIRED_ASPECTS =
      Set.of(
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          Constants.DOMAINS_ASPECT_NAME);

  private AspectPluginConfig config;

  public DatasetGovernanceValidator() {
    logger.warn(
        "VALIDATOR CONSTRUCTOR CALLED - DatasetGovernanceValidator initialized for logical datasets with required aspects: {}",
        REQUIRED_ASPECTS);
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    logger.warn("VALIDATOR CALLED - validateProposedAspects with {} MCP items", mcpItems.size());
    mcpItems.forEach(
        item ->
            logger.warn(
                "MCP Item: urn={}, aspectName={}, entityType={}",
                item.getUrn(),
                item.getAspectName(),
                item.getUrn().getEntityType()));

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    // Group all logical dataset URNs in the batch
    Set<Urn> datasetUrns =
        mcpItems.stream()
            .map(BatchItem::getUrn)
            .filter(urn -> DATASET_ENTITY_TYPE.equals(urn.getEntityType()))
            .filter(this::isLogicalDataPlatform)
            .collect(Collectors.toSet());

    if (datasetUrns.isEmpty()) {
      logger.debug("No logical dataset URNs found in batch, skipping validation");
      return Stream.empty();
    }

    logger.debug("Found {} logical dataset URNs to validate: {}", datasetUrns.size(), datasetUrns);

    // Create aspects-per-URN map for batch fetching existing aspects
    Map<Urn, Set<String>> urnAspects =
        datasetUrns.stream().collect(Collectors.toMap(urn -> urn, urn -> REQUIRED_ASPECTS));

    // Batch fetch existing aspects from GMS
    Map<Urn, Map<String, SystemAspect>> existingAspects =
        aspectRetriever.getLatestSystemAspects(urnAspects);

    // Track which aspects are being set in this batch for logical datasets
    Map<Urn, Set<String>> batchAspects =
        mcpItems.stream()
            .filter(item -> DATASET_ENTITY_TYPE.equals(item.getUrn().getEntityType()))
            .filter(item -> isLogicalDataPlatform(item.getUrn()))
            .filter(item -> REQUIRED_ASPECTS.contains(item.getAspectName()))
            .collect(
                Collectors.groupingBy(
                    BatchItem::getUrn,
                    Collectors.mapping(BatchItem::getAspectName, Collectors.toSet())));

    logger.debug("Aspects being set in batch: {}", batchAspects);

    // Validate each dataset URN
    return datasetUrns.stream()
        .map(
            urn ->
                validateDatasetGovernance(
                    urn, existingAspects.get(urn), batchAspects.get(urn), mcpItems))
        .filter(Objects::nonNull);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    
    // For this simple governance validator, we only need proposed validation
    // since we're just checking if required aspects exist
    return Stream.empty();
  }

  /**
   * Validates that a logical dataset has all required governance aspects either existing in GMS or
   * being set in the current batch.
   *
   * @param datasetUrn The logical dataset URN to validate
   * @param existingAspects Map of existing aspects from GMS (can be null if none exist)
   * @param batchAspects Set of aspects being set in current batch (can be null if none)
   * @param mcpItems The MCP items for context
   * @return AspectValidationException if validation fails, null if passes
   */
  private AspectValidationException validateDatasetGovernance(
      Urn datasetUrn,
      Map<String, SystemAspect> existingAspects,
      Set<String> batchAspects,
      Collection<? extends BatchItem> mcpItems) {

    Set<String> missingAspects =
        REQUIRED_ASPECTS.stream()
            .filter(
                aspectName -> {
                  // Check if aspect exists in GMS
                  boolean existsInGMS =
                      existingAspects != null && existingAspects.containsKey(aspectName);
                  // Check if aspect is being set in this batch
                  boolean existsInBatch = batchAspects != null && batchAspects.contains(aspectName);

                  boolean isMissing = !existsInGMS && !existsInBatch;

                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Dataset {} aspect {}: existsInGMS={}, existsInBatch={}, missing={}",
                        datasetUrn,
                        aspectName,
                        existsInGMS,
                        existsInBatch,
                        isMissing);
                  }

                  return isMissing;
                })
            .collect(Collectors.toSet());

    if (!missingAspects.isEmpty()) {
      String message =
          String.format(
              "Logical dataset %s is missing required governance aspects: %s. "
                  + "All logical datasets must have ownership, tags, and domain set.",
              datasetUrn, String.join(", ", missingAspects));

      logger.warn("Validation failed for dataset {}: {}", datasetUrn, message);

      // Find a relevant BatchItem for this dataset to include in the exception
      BatchItem relevantItem =
          mcpItems.stream()
              .filter(
                  item ->
                      DATASET_ENTITY_TYPE.equals(item.getUrn().getEntityType())
                          && datasetUrn.equals(item.getUrn()))
              .findFirst()
              .orElse(null);

      return AspectValidationException.forItem(relevantItem, message);
    }

    logger.debug("Validation passed for dataset {}", datasetUrn);
    return null;
  }

  @Nonnull
  @Override
  public AspectPluginConfig getConfig() {
    return config;
  }

  @Override
  public DatasetGovernanceValidator setConfig(@Nonnull AspectPluginConfig config) {
    this.config = config;
    logger.warn(
        "VALIDATOR CONFIG SET - supportedOperations: {}, supportedEntityAspectNames: {}",
        config.getSupportedOperations(),
        config.getSupportedEntityAspectNames());
    logger.warn(
        "CONFIG DETAILS - enabled: {}, className: {}",
        config.isEnabled(),
        config.getClassName());
    return this;
  }

  /**
   * Checks if a dataset URN uses the logical data platform. Dataset URNs have the format:
   * urn:li:dataset:(urn:li:dataPlatform:PLATFORM,dataset.name,ENV)
   */
  private boolean isLogicalDataPlatform(Urn datasetUrn) {
    String urnString = datasetUrn.toString();
    return urnString.contains(LOGICAL_DATA_PLATFORM);
  }
}
