package com.linkedin.metadata.test.util;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.exception.SelectionTooLargeException;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestUtils {

  /**
   * Returns the entity types that support Metadata Tests, based on the presence of the required
   * "testResults" aspect.
   *
   * @param entityRegistry the entity registry
   */
  public static Set<String> getSupportedEntityTypes(final EntityRegistry entityRegistry) {
    if (entityRegistry == null) {
      return Collections.emptySet();
    }
    return entityRegistry.getEntitySpecs().values().stream()
        .filter(value -> value.hasAspect(Constants.TEST_RESULTS_ASPECT_NAME))
        .map(EntitySpec::getName)
        .collect(Collectors.toSet());
  }

  @Nonnull
  public static MetadataChangeProposal buildProposalForUrn(
      Urn entityUrn, String aspectName, RecordTemplate recordTemplate) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityUrn);
    return setProposalProperties(proposal, entityUrn.getEntityType(), aspectName, recordTemplate);
  }

  @Nonnull
  private static MetadataChangeProposal setProposalProperties(
      @Nonnull MetadataChangeProposal proposal,
      @Nonnull String entityType,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect) {
    proposal.setEntityType(entityType);
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);

    // Assumes proposal is generated first without system metadata
    SystemMetadata systemMetadata = createDefaultSystemMetadata();
    StringMap properties = new StringMap();
    properties.put(Constants.APP_SOURCE, Constants.METADATA_TESTS_SOURCE);
    systemMetadata.setProperties(properties);
    proposal.setSystemMetadata(systemMetadata);
    return proposal;
  }

  public static Filter buildTestPassingFilter(
      @Nonnull final Urn testUrn,
      @Nullable String testDefinitionMd5,
      @Nullable AspectRetriever aspectRetriever) {
    return buildFilter(
        testUrn, PASSING_TESTS_FIELD, testDefinitionMd5, PASSING_TESTS_MD5_FIELD, aspectRetriever);
  }

  public static Filter buildTestFailingFilter(
      @Nonnull final Urn testUrn,
      @Nullable String testDefinitionMd5,
      @Nullable AspectRetriever aspectRetriever) {
    return buildFilter(
        testUrn, FAILING_TESTS_FIELD, testDefinitionMd5, FAILING_TESTS_MD5_FIELD, aspectRetriever);
  }

  private static Filter buildFilter(
      @Nonnull final Urn testUrn,
      @Nonnull final String fieldName,
      @Nullable String testDefinitionMd5,
      @Nonnull String md5FieldName,
      @Nullable AspectRetriever aspectRetriever) {
    final Filter result = new Filter();
    final String fieldNameWithSuffix = ESUtils.toKeywordField(fieldName, false, aspectRetriever);
    final Criterion urnCriterion =
        CriterionUtils.buildCriterion(fieldNameWithSuffix, Condition.EQUAL, testUrn.toString());
    final Criterion md5Criterion =
        testDefinitionMd5 == null
            ? null
            : CriterionUtils.buildCriterion(
                ESUtils.toKeywordField(md5FieldName, false, aspectRetriever),
                Condition.EQUAL,
                testDefinitionMd5);

    CriterionArray criterionArray = new CriterionArray(urnCriterion);
    if (md5Criterion != null) {
      criterionArray.add(md5Criterion);
    }
    result.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(criterionArray))));
    log.info("Filter: {}", result.toString());
    return result;
  }

  public static SelectionTooLargeException abortBeyondLimitExecution(
      TestDefinition testDefinition, int count) {
    return new SelectionTooLargeException(
        String.format("Too many entities selected for test %s, count: %s", testDefinition, count),
        count);
  }

  private TestUtils() {}
}
