package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.Utils.platformInstanceUrn;
import static io.datahubproject.iceberg.catalog.Utils.platformUrn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IcbergBatchTest {
  @Mock private OperationContext mockOperationContext;

  @Mock private RetrieverContext mockRetrieverContext;

  private Urn testActorUrn = new CorpuserUrn("urn:li:corpuser:testUser");

  private IcebergBatch icebergBatch;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    ActorContext actorContext = mock(ActorContext.class);
    when(mockOperationContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(testActorUrn);

    when(mockOperationContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    icebergBatch = new IcebergBatch(mockOperationContext);
  }

  @Test
  public void testSoftDelete() {
    Urn urn = mock(Urn.class);
    String entityName = "SOME_ENTITY";
    icebergBatch.softDeleteEntity(urn, entityName);
    assertEquals(icebergBatch.getMcps().size(), 1);
    assertEquals(
        icebergBatch.getMcps().get(0),
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(true)))
            .setChangeType(ChangeType.UPSERT));
  }

  @Test
  public void testCreateBatch() {
    Urn urn = mock(Urn.class);
    String entityName = "SOME_ENTITY";
    RecordTemplate creationAspect = new Aspect(new DataMap(Map.of("someKey", "someValue")));

    IcebergBatch.EntityBatch entityBatch =
        icebergBatch.createEntity(urn, entityName, "CREATION_ASPECT", creationAspect);

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("CREATION_ASPECT")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(creationAspect))
            .setChangeType(ChangeType.CREATE_ENTITY));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(false)))
            .setChangeType(ChangeType.CREATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);

    entityBatch.platformInstance("some_platform");
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(
                    new DataPlatformInstance()
                        .setPlatform(platformUrn())
                        .setInstance(platformInstanceUrn("some_platform"))))
            .setChangeType(ChangeType.CREATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);

    entityBatch.aspect(
        "some_aspect", new Aspect(new DataMap(Map.of("anotherKey", "anotherValue"))));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("some_aspect")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(new Aspect(new DataMap(Map.of("anotherKey", "anotherValue")))))
            .setChangeType(ChangeType.CREATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);
  }

  @Test
  public void testUpdateBatch() {
    Urn urn = mock(Urn.class);
    String entityName = "SOME_ENTITY";

    IcebergBatch.EntityBatch entityBatch = icebergBatch.updateEntity(urn, entityName);
    assertTrue(icebergBatch.getMcps().isEmpty());

    try {
      entityBatch.platformInstance("some_platform");
      fail();
    } catch (UnsupportedOperationException e) {

    }

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    entityBatch.aspect("some_aspect", new Aspect(new DataMap(Map.of("someKey", "someValue"))));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("some_aspect")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Aspect(new DataMap(Map.of("someKey", "someValue")))))
            .setChangeType(ChangeType.UPDATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);

    entityBatch.aspect(
        "some_other_aspect", new Aspect(new DataMap(Map.of("anotherKey", "anotherValue"))));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("some_other_aspect")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(new Aspect(new DataMap(Map.of("anotherKey", "anotherValue")))))
            .setChangeType(ChangeType.UPDATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);
  }

  @Test
  public void testConditionalUpdate() {
    Urn urn = mock(Urn.class);
    String entityName = "SOME_ENTITY";

    IcebergBatch.EntityBatch entityBatch =
        icebergBatch.conditionalUpdateEntity(
            urn,
            entityName,
            "COND_ASPECT",
            new Aspect(new DataMap(Map.of("someKey", "someValue"))),
            "version1");

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("COND_ASPECT")
            .setHeaders(
                new StringMap(
                    Map.of(
                        SYNC_INDEX_UPDATE_HEADER_NAME,
                        Boolean.toString(true),
                        HTTP_HEADER_IF_VERSION_MATCH,
                        "version1")))
            .setAspect(serializeAspect(new Aspect(new DataMap(Map.of("someKey", "someValue")))))
            .setChangeType(ChangeType.UPDATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);

    try {
      entityBatch.platformInstance("some_platform");
      fail();
    } catch (UnsupportedOperationException e) {

    }

    entityBatch.aspect(
        "some_other_aspect", new Aspect(new DataMap(Map.of("anotherKey", "anotherValue"))));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("some_other_aspect")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(new Aspect(new DataMap(Map.of("anotherKey", "anotherValue")))))
            .setChangeType(ChangeType.UPDATE));
    assertEquals(icebergBatch.getMcps(), expectedMcps);
  }

  @Test
  public void testMixedBatch() {
    Urn urn = mock(Urn.class);
    String entityName = "SOME_ENTITY";
    RecordTemplate creationAspect = new Aspect(new DataMap(Map.of("someKey", "someValue")));

    IcebergBatch.EntityBatch createEntityBatch =
        icebergBatch.createEntity(urn, entityName, "CREATION_ASPECT", creationAspect);
    IcebergBatch.EntityBatch updateEntityBatch = icebergBatch.updateEntity(urn, entityName);
    IcebergBatch.EntityBatch condUpdateEntityBatch =
        icebergBatch.conditionalUpdateEntity(
            urn,
            entityName,
            "COND_ASPECT",
            new Aspect(new DataMap(Map.of("someKey", "someValue"))),
            "version1");

    createEntityBatch.platformInstance("some_platform");
    updateEntityBatch.aspect("aspect1", new Aspect(new DataMap(Map.of("key1", "value1"))));
    createEntityBatch.aspect("aspect2", new Aspect(new DataMap(Map.of("key2", "value2"))));
    condUpdateEntityBatch.aspect("aspect3", new Aspect(new DataMap(Map.of("key3", "value3"))));
    icebergBatch.softDeleteEntity(urn, entityName);

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("CREATION_ASPECT")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(creationAspect))
            .setChangeType(ChangeType.CREATE_ENTITY));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(false)))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("COND_ASPECT")
            .setHeaders(
                new StringMap(
                    Map.of(
                        SYNC_INDEX_UPDATE_HEADER_NAME,
                        Boolean.toString(true),
                        HTTP_HEADER_IF_VERSION_MATCH,
                        "version1")))
            .setAspect(serializeAspect(new Aspect(new DataMap(Map.of("someKey", "someValue")))))
            .setChangeType(ChangeType.UPDATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(
                    new DataPlatformInstance()
                        .setPlatform(platformUrn())
                        .setInstance(platformInstanceUrn("some_platform"))))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("aspect1")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Aspect(new DataMap(Map.of("key1", "value1")))))
            .setChangeType(ChangeType.UPDATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("aspect2")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Aspect(new DataMap(Map.of("key2", "value2")))))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName("aspect3")
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Aspect(new DataMap(Map.of("key3", "value3")))))
            .setChangeType(ChangeType.UPDATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(entityName)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(true)))
            .setChangeType(ChangeType.UPSERT));

    assertEquals(icebergBatch.getMcps(), expectedMcps);
  }

  @Test
  public void testAsBatch() {
    AspectsBatchImpl markerBatch = mock(AspectsBatchImpl.class);
    AspectsBatchImpl.AspectsBatchImplBuilder builder =
        mock(AspectsBatchImpl.AspectsBatchImplBuilder.class);
    when(builder.mcps(
            same(icebergBatch.getMcps()), any(AuditStamp.class), same(mockRetrieverContext)))
        .thenReturn(builder);
    when(builder.build()).thenReturn(markerBatch);

    try (MockedStatic<AspectsBatchImpl> stsClientMockedStatic =
        mockStatic(AspectsBatchImpl.class)) {
      stsClientMockedStatic.when(AspectsBatchImpl::builder).thenReturn(builder);
      AspectsBatch actual = icebergBatch.asAspectsBatch();
      assertSame(actual, markerBatch);
    }

    ArgumentCaptor<AuditStamp> auditStampArg = ArgumentCaptor.forClass(AuditStamp.class);
    verify(builder)
        .mcps(same(icebergBatch.getMcps()), auditStampArg.capture(), same(mockRetrieverContext));
    assertSame(auditStampArg.getValue().getActor(), testActorUrn);
  }
}
