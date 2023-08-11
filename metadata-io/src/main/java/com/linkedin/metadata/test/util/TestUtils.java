package com.linkedin.metadata.test.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;


@Slf4j
public class TestUtils {

  /**
   * Returns the entity types that support Metadata Tests,
   * based on the presence of the required "testResults" aspect.
   *
   * @param entityRegistry the entity registry
   */
  public static Set<String> getSupportedEntityTypes(final EntityRegistry entityRegistry) {
    if (entityRegistry == null) {
      return Collections.emptySet();
    }
    return entityRegistry.getEntitySpecs().values().stream().filter(value -> value.hasAspect(Constants.TEST_RESULTS_ASPECT_NAME))
      .map(EntitySpec::getName)
      .collect(Collectors.toSet());
  }

  @Nonnull
  public static MetadataChangeProposal buildProposalForUrn(
          Urn entityUrn,
          String aspectName,
          RecordTemplate recordTemplate
  ) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityUrn);
    return setProposalProperties(
            proposal,
            entityUrn.getEntityType(),
            aspectName,
            recordTemplate
    );
  }

  @Nonnull
  private static MetadataChangeProposal setProposalProperties(
          @Nonnull MetadataChangeProposal proposal,
          @Nonnull String entityType,
          @Nonnull String aspectName,
          @Nonnull RecordTemplate aspect
  ) {
    proposal.setEntityType(entityType);
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);

    // Assumes proposal is generated first without system metadata
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(Constants.APP_SOURCE, Constants.METADATA_TESTS_SOURCE);
    systemMetadata.setProperties(properties);
    proposal.setSystemMetadata(systemMetadata);
    return proposal;
  }

  private TestUtils() {
  }

}
