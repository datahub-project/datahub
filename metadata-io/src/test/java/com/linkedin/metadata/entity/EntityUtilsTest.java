package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityUtilsTest {

  @Mock private EntityService<?> entityService;

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testGetUrnFromString_ValidUrn() {
    String validUrnStr = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)";
    Urn urn = EntityUtils.getUrnFromString(validUrnStr);
    assertNotNull(urn);
    assertEquals(urn.toString(), validUrnStr);
  }

  @Test
  public void testGetUrnFromString_InvalidUrn() {
    String invalidUrnStr = "invalid:urn:format";
    Urn urn = EntityUtils.getUrnFromString(invalidUrnStr);
    assertNull(urn);
  }

  @Test
  public void testGetAuditStamp() throws URISyntaxException {
    Urn actorUrn = Urn.createFromString("urn:li:corpuser:test");
    AuditStamp auditStamp = EntityUtils.getAuditStamp(actorUrn);

    assertNotNull(auditStamp);
    assertEquals(auditStamp.getActor(), actorUrn);
    assertTrue(auditStamp.getTime() > 0);
  }

  @Test
  public void testIngestChangeProposals() {
    List<MetadataChangeProposal> changes = new ArrayList<>();
    Urn actor = EntityUtils.getUrnFromString("urn:li:corpuser:testUser");

    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, true);

    verify(entityService, times(1)).ingestProposal(eq(opContext), any(), eq(true));
  }

  @Test
  public void testGetAspectFromEntity_Success() {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)";
    String aspectName = "testAspect";
    MockRecordTemplate mockAspect = new MockRecordTemplate();

    when(entityService.getAspect(
            any(OperationContext.class), any(Urn.class), eq(aspectName), eq(0L)))
        .thenReturn(mockAspect);

    MockRecordTemplate defaultValue = new MockRecordTemplate();
    MockRecordTemplate result =
        (MockRecordTemplate)
            EntityUtils.getAspectFromEntity(
                opContext, entityUrn, aspectName, entityService, defaultValue);

    assertNotNull(result);
    assertEquals(result, mockAspect);
    verify(entityService)
        .getAspect(
            argThat(
                ctx ->
                    ctx.getPrimaryStorageContext().getReadPreference() == ReadPreference.PRIMARY),
            any(Urn.class),
            eq(aspectName),
            eq(0L));
  }

  @Test
  public void testGetAspectFromEntity_InvalidUrn() {
    String invalidUrn = "invalid:urn";
    String aspectName = "testAspect";
    MockRecordTemplate defaultValue = new MockRecordTemplate();

    MockRecordTemplate result =
        (MockRecordTemplate)
            EntityUtils.getAspectFromEntity(
                opContext, invalidUrn, aspectName, entityService, defaultValue);

    assertEquals(result, defaultValue);
  }

  @Test
  public void testGetAspectFromEntity_NullAspect() {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)";
    String aspectName = "testAspect";
    MockRecordTemplate defaultValue = new MockRecordTemplate();

    when(entityService.getAspect(
            any(OperationContext.class), any(Urn.class), eq(aspectName), eq(0L)))
        .thenReturn(null);

    MockRecordTemplate result =
        (MockRecordTemplate)
            EntityUtils.getAspectFromEntity(
                opContext, entityUrn, aspectName, entityService, defaultValue);

    assertEquals(result, defaultValue);
  }

  @Test
  public void testToSystemAspect_NullEntityAspect() {
    var result =
        EntityUtils.toSystemAspect(opContext, opContext.getRetrieverContext(), null, false);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testToSystemAspects_SkipsRowsUnknownToRegistry() {
    String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)";
    EntityAspect known = aspectRow(datasetUrn, STATUS_ASPECT_NAME, "{\"removed\":false}");
    EntityAspect unknownAspect = aspectRow(datasetUrn, "aspectFromNewerBuild", "{\"a\":1}");
    EntityAspect unknownEntity =
        aspectRow("urn:li:entityFromNewerBuild:abc", STATUS_ASPECT_NAME, "{\"removed\":false}");

    List<SystemAspect> result =
        EntityUtils.toSystemAspects(
            opContext,
            opContext.getRetrieverContext(),
            List.of(unknownAspect, known, unknownEntity));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getUrn().toString(), datasetUrn);
    assertEquals(result.get(0).getAspectName(), STATUS_ASPECT_NAME);
    assertFalse(((Status) result.get(0).getRecordTemplate()).isRemoved());
  }

  @Test
  public void testToSystemAspects_StripsReferencesToUnknownEntityTypes() {
    String dataset = "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)";
    EntityAspect row =
        aspectRow(
            "urn:li:dataProduct:dp",
            DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
            "{\"assets\":[{\"destinationUrn\":\""
                + dataset
                + "\"},{\"destinationUrn\":\"urn:li:entityFromNewerBuild:x\"}]}");

    List<SystemAspect> result =
        EntityUtils.toSystemAspects(opContext, opContext.getRetrieverContext(), List.of(row));

    DataProductProperties properties = (DataProductProperties) result.get(0).getRecordTemplate();
    assertEquals(properties.getAssets().size(), 1);
    assertEquals(properties.getAssets().get(0).getDestinationUrn().toString(), dataset);
  }

  @Test
  public void testToSystemAspect_UnknownAspectIsEmpty() {
    EntityAspect unknownAspect =
        aspectRow(
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)",
            "aspectFromNewerBuild",
            "{\"a\":1}");

    assertTrue(
        EntityUtils.toSystemAspect(opContext, opContext.getRetrieverContext(), unknownAspect)
            .isEmpty());
  }

  private static EntityAspect aspectRow(String urn, String aspectName, String metadata) {
    EntityAspect row = new EntityAspect();
    row.setUrn(urn);
    row.setAspect(aspectName);
    row.setVersion(0L);
    row.setMetadata(metadata);
    row.setCreatedOn(new Timestamp(1_700_000_000_000L));
    row.setCreatedBy("urn:li:corpuser:datahub");
    return row;
  }

  @Test
  public void testCalculateNextVersions_EmptyInput() {
    TransactionContext txContext = mock(TransactionContext.class);
    AspectDao aspectDao = mock(AspectDao.class);
    Map<String, Map<String, SystemAspect>> latestAspects = new HashMap<>();
    Map<String, Set<String>> urnAspects = new HashMap<>();

    OperationContext opContext = mock(OperationContext.class);
    Map<String, Map<String, Long>> result =
        EntityUtils.calculateNextVersions(
            opContext, txContext, aspectDao, latestAspects, urnAspects);

    assertTrue(result.isEmpty());
  }

  private static class MockRecordTemplate extends com.linkedin.data.template.RecordTemplate {
    public MockRecordTemplate() {
      super(new com.linkedin.data.DataMap(), null);
    }
  }
}
