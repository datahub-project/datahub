package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entitytype.EntityTypeInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class IngestEntityTypesStepTest {

  @BeforeTest
  public static void beforeTest() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testExecuteTestEntityRegistry() throws Exception {
    EntityRegistry testEntityRegistry = getTestEntityRegistry();
    final EntityService<?> entityService = mock(EntityService.class);
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    final IngestEntityTypesStep step = new IngestEntityTypesStep(entityService);

    step.execute(opContext);

    Urn userUrn =
        Urn.createFromString(String.format("urn:li:entityType:datahub.%s", CORP_USER_ENTITY_NAME));
    EntityTypeInfo userInfo = new EntityTypeInfo();
    userInfo.setDisplayName("corpuser");
    userInfo.setQualifiedName("datahub.corpuser");

    Urn chartUrn =
        Urn.createFromString(String.format("urn:li:entityType:datahub.%s", CHART_ENTITY_NAME));
    EntityTypeInfo chartInfo = new EntityTypeInfo();
    chartInfo.setDisplayName("chart");
    chartInfo.setQualifiedName("datahub.chart");

    Urn dataPlatformUrn =
        Urn.createFromString(
            String.format("urn:li:entityType:datahub.%s", DATA_PLATFORM_ENTITY_NAME));
    EntityTypeInfo dataPlatformInfo = new EntityTypeInfo();
    dataPlatformInfo.setDisplayName("dataPlatform");
    dataPlatformInfo.setQualifiedName("datahub.dataPlatform");

    // Verify all entities were ingested.
    Mockito.verify(entityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(buildUpdateEntityTypeProposal(userUrn, userInfo)),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));

    Mockito.verify(entityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(buildUpdateEntityTypeProposal(chartUrn, chartInfo)),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));

    Mockito.verify(entityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(buildUpdateEntityTypeProposal(dataPlatformUrn, dataPlatformInfo)),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  private static MetadataChangeProposal buildUpdateEntityTypeProposal(
      final Urn entityTypeUrn, final EntityTypeInfo info) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(entityTypeUrn);
    mcp.setEntityType(ENTITY_TYPE_ENTITY_NAME);
    mcp.setAspectName(ENTITY_TYPE_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));
    return mcp;
  }

  @NotNull
  private ConfigEntityRegistry getTestEntityRegistry() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    return new ConfigEntityRegistry(
        IngestDataPlatformInstancesStepTest.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }
}
