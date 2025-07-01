package com.linkedin.datahub.upgrade.schemafield;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.schemafield.GenerateSchemaFieldsFromSchemaMetadataStep;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GenerateSchemaFieldsFromSchemaMetadataStepTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityService<?> mockEntityService;

  @Mock private AspectDao mockAspectDao;

  @Mock private RetrieverContext mockRetrieverContext;

  private GenerateSchemaFieldsFromSchemaMetadataStep step;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    step =
        new GenerateSchemaFieldsFromSchemaMetadataStep(
            mockOpContext, mockEntityService, mockAspectDao, 10, 100, 1000);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  /** Test to verify the correct step ID is returned. */
  @Test
  public void testId() {
    assertEquals("schema-field-from-schema-metadata-v1", step.id());
  }

  /** Test to verify the skip logic based on the environment variable. */
  @Test
  public void testSkip() {
    UpgradeContext mockContext = mock(UpgradeContext.class);
    System.setProperty("SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA", "true");
    assertTrue(step.skip(mockContext));

    System.setProperty("SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA", "false");
    assertFalse(step.skip(mockContext));
  }

  /** Test to verify the correct URN pattern is returned. */
  @Test
  public void testGetUrnLike() {
    assertEquals("urn:li:dataset:%", step.getUrnLike());
  }

  /**
   * Test to verify the executable function processes batches correctly and returns a success
   * result.
   */
  @Test
  public void testExecutable() {
    UpgradeContext mockContext = mock(UpgradeContext.class);

    EbeanAspectV2 mockAspect = mock(EbeanAspectV2.class);
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(any(RestoreIndicesArgs.class))).thenReturn(mockStream);

    when(mockStream.partition(anyInt())).thenReturn(Stream.of(Stream.of(mockAspect)));

    SystemAspect mockSystemAspect = mock(SystemAspect.class);
    when(mockSystemAspect.getAspectName()).thenReturn("schemaMetadata");
    when(mockSystemAspect.getAspect(SchemaMetadata.class)).thenReturn(new SchemaMetadata());

    // when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockSystemAspect);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);

    UpgradeStepResult result = step.executable().apply(mockContext);
    assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());

    verify(mockAspectDao).streamAspectBatches(argsCaptor.capture());
    assertEquals("schemaMetadata", argsCaptor.getValue().aspectName());
    assertEquals(10, argsCaptor.getValue().batchSize());
    assertEquals(1000, argsCaptor.getValue().limit());
  }

  // Additional tests can be added to cover more scenarios and edge cases
}
