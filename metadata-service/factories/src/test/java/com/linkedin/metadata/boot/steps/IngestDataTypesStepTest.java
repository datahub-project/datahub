package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datatype.DataTypeInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.EntityRegistryContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IngestDataTypesStepTest {

  private static final Urn TEST_DATA_TYPE_URN = UrnUtils.getUrn("urn:li:dataType:datahub.test");

  @Test
  public void testExecuteValidDataTypesNoExistingDataTypes() throws Exception {
    EntityRegistry testEntityRegistry = getTestEntityRegistry();
    final EntityService<?> entityService = mock(EntityService.class);

    final OperationContext mockContext = mock(OperationContext.class);
    final EntityRegistryContext entityRegistryContext = mock(EntityRegistryContext.class);
    when(mockContext.getEntityRegistryContext()).thenReturn(entityRegistryContext);
    when(mockContext.getEntityRegistry()).thenReturn(testEntityRegistry);
    when(entityRegistryContext.getKeyAspectSpec(anyString()))
        .thenAnswer(
            args -> testEntityRegistry.getEntitySpec(args.getArgument(0)).getKeyAspectSpec());

    final IngestDataTypesStep step =
        new IngestDataTypesStep(entityService, "./boot/test_data_types_valid.json");

    step.execute(mockContext);

    DataTypeInfo expectedResult = new DataTypeInfo();
    expectedResult.setDescription("Test Description");
    expectedResult.setDisplayName("Test Name");
    expectedResult.setQualifiedName("datahub.test");

    Mockito.verify(entityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(buildUpdateDataTypeProposal(expectedResult)),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  @Test
  public void testExecuteInvalidJson() throws Exception {
    final EntityService<?> entityService = mock(EntityService.class);
    final OperationContext mockContext = mock(OperationContext.class);
    when(mockContext.getEntityRegistry()).thenReturn(mock(EntityRegistry.class));

    when(entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> Set.of());

    final IngestDataTypesStep step =
        new IngestDataTypesStep(entityService, "./boot/test_data_types_invalid.json");

    Assert.assertThrows(RuntimeException.class, () -> step.execute(mockContext));

    verify(entityService, times(1)).exists(any(OperationContext.class), any(Collection.class));

    // Verify no additional interactions
    verifyNoMoreInteractions(entityService);
  }

  private static MetadataChangeProposal buildUpdateDataTypeProposal(final DataTypeInfo info) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_DATA_TYPE_URN);
    mcp.setEntityType(DATA_TYPE_ENTITY_NAME);
    mcp.setAspectName(DATA_TYPE_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));
    return mcp;
  }

  @NotNull
  private ConfigEntityRegistry getTestEntityRegistry() {
    return new ConfigEntityRegistry(
        IngestDataPlatformInstancesStepTest.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }
}
