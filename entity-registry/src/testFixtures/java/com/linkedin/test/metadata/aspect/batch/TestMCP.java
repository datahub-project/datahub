package com.linkedin.test.metadata.aspect.batch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder(toBuilder = true)
@Getter
public class TestMCP implements ChangeMCP {
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:datahub,Test,PROD)";

  public static <T extends RecordTemplate> Collection<ReadItem> ofOneBatchItem(
      Urn urn, T aspect, EntityRegistry entityRegistry) {
    return Set.of(
        TestMCP.builder()
            .urn(urn)
            .entitySpec(entityRegistry.getEntitySpec(urn.getEntityType()))
            .aspectSpec(
                entityRegistry.getAspectSpecs().get(TestEntityRegistry.getAspectName(aspect)))
            .recordTemplate(aspect)
            .build());
  }

  public static <T extends RecordTemplate> Collection<ReadItem> ofOneBatchItemDatasetUrn(
      T aspect, EntityRegistry entityRegistry) {
    try {
      return ofOneBatchItem(Urn.createFromString(TEST_DATASET_URN), aspect, entityRegistry);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends RecordTemplate> Set<BatchItem> ofOneUpsertItem(
      Urn urn, T aspect, EntityRegistry entityRegistry) {
    return Set.of(
        TestMCP.builder()
            .urn(urn)
            .entitySpec(entityRegistry.getEntitySpec(urn.getEntityType()))
            .aspectSpec(
                entityRegistry.getAspectSpecs().get(TestEntityRegistry.getAspectName(aspect)))
            .recordTemplate(aspect)
            .build());
  }

  public static <T extends RecordTemplate> Set<BatchItem> ofOneUpsertItemDatasetUrn(
      T aspect, EntityRegistry entityRegistry) {
    try {
      return ofOneUpsertItem(Urn.createFromString(TEST_DATASET_URN), aspect, entityRegistry);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends RecordTemplate> Set<ChangeMCP> ofOneMCP(
      Urn urn, T newAspect, EntityRegistry entityRegistry) {
    return ofOneMCP(urn, null, newAspect, entityRegistry);
  }

  public static <T extends RecordTemplate> Set<ChangeMCP> ofOneMCP(
      Urn urn, @Nullable T oldAspect, T newAspect, EntityRegistry entityRegistry) {

    SystemAspect mockNewSystemAspect = mock(SystemAspect.class);
    when(mockNewSystemAspect.getRecordTemplate()).thenReturn(newAspect);
    when(mockNewSystemAspect.getAspect(any(Class.class)))
        .thenAnswer(args -> ReadItem.getAspect(args.getArgument(0), newAspect));

    SystemAspect mockOldSystemAspect = null;
    if (oldAspect != null) {
      mockOldSystemAspect = mock(SystemAspect.class);
      when(mockOldSystemAspect.getRecordTemplate()).thenReturn(oldAspect);
      when(mockOldSystemAspect.getAspect(any(Class.class)))
          .thenAnswer(args -> ReadItem.getAspect(args.getArgument(0), oldAspect));
    }

    return Set.of(
        TestMCP.builder()
            .urn(urn)
            .entitySpec(entityRegistry.getEntitySpec(urn.getEntityType()))
            .aspectSpec(
                entityRegistry.getAspectSpecs().get(TestEntityRegistry.getAspectName(newAspect)))
            .recordTemplate(newAspect)
            .systemAspect(mockNewSystemAspect)
            .previousSystemAspect(mockOldSystemAspect)
            .build());
  }

  private Urn urn;
  private RecordTemplate recordTemplate;
  @Setter private SystemMetadata systemMetadata;
  private AuditStamp auditStamp;
  private ChangeType changeType;
  @Nonnull private final EntitySpec entitySpec;
  @Nonnull private final AspectSpec aspectSpec;
  private SystemAspect systemAspect;
  private MetadataChangeProposal metadataChangeProposal;
  @Setter private SystemAspect previousSystemAspect;
  @Setter private long nextAspectVersion;
  @Setter private Map<String, String> headers;

  @Override
  public Map<String, String> getHeaders() {
    return Optional.ofNullable(metadataChangeProposal)
        .filter(MetadataChangeProposal::hasHeaders)
        .map(
            mcp ->
                mcp.getHeaders().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .orElse(headers != null ? headers : Collections.emptyMap());
  }

  @Override
  public boolean isDatabaseDuplicateOf(BatchItem other) {
    return equals(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestMCP testMCP = (TestMCP) o;
    return urn.equals(testMCP.urn)
        && DataTemplateUtil.areEqual(recordTemplate, testMCP.recordTemplate)
        && Objects.equals(systemAspect, testMCP.systemAspect)
        && Objects.equals(previousSystemAspect, testMCP.previousSystemAspect)
        && Objects.equals(auditStamp, testMCP.auditStamp)
        && Objects.equals(changeType, testMCP.changeType)
        && Objects.equals(metadataChangeProposal, testMCP.metadataChangeProposal);
  }

  @Override
  public int hashCode() {
    int result = urn.hashCode();
    result = 31 * result + Objects.hashCode(recordTemplate);
    result = 31 * result + Objects.hashCode(systemAspect);
    result = 31 * result + Objects.hashCode(previousSystemAspect);
    result = 31 * result + Objects.hashCode(auditStamp);
    result = 31 * result + Objects.hashCode(changeType);
    result = 31 * result + Objects.hashCode(metadataChangeProposal);
    return result;
  }
}
