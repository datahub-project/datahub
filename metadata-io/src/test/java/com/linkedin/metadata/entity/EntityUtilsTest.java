package com.linkedin.metadata.entity;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
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

    when(entityService.getAspect(eq(opContext), any(Urn.class), eq(aspectName), eq(0L)))
        .thenReturn(mockAspect);

    MockRecordTemplate defaultValue = new MockRecordTemplate();
    MockRecordTemplate result =
        (MockRecordTemplate)
            EntityUtils.getAspectFromEntity(
                opContext, entityUrn, aspectName, entityService, defaultValue);

    assertNotNull(result);
    assertEquals(result, mockAspect);
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

    when(entityService.getAspect(eq(opContext), any(Urn.class), eq(aspectName), eq(0L)))
        .thenReturn(null);

    MockRecordTemplate result =
        (MockRecordTemplate)
            EntityUtils.getAspectFromEntity(
                opContext, entityUrn, aspectName, entityService, defaultValue);

    assertEquals(result, defaultValue);
  }

  @Test
  public void testToSystemAspect_NullEntityAspect() {
    var result = EntityUtils.toSystemAspect(opContext.getRetrieverContext(), null, false);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCalculateNextVersions_EmptyInput() {
    TransactionContext txContext = mock(TransactionContext.class);
    AspectDao aspectDao = mock(AspectDao.class);
    Map<String, Map<String, SystemAspect>> latestAspects = new HashMap<>();
    Map<String, Set<String>> urnAspects = new HashMap<>();

    Map<String, Map<String, Long>> result =
        EntityUtils.calculateNextVersions(txContext, aspectDao, latestAspects, urnAspects);

    assertTrue(result.isEmpty());
  }

  private static class MockRecordTemplate extends com.linkedin.data.template.RecordTemplate {
    public MockRecordTemplate() {
      super(new com.linkedin.data.DataMap(), null);
    }
  }
}
