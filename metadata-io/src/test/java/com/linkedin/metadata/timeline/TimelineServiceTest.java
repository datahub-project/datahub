package com.linkedin.metadata.timeline;

import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * A class to test {@link TimelineServiceImpl}
 *
 * <p>This class is generic to allow same integration tests to be reused to test all supported
 * storage backends. If you're adding another storage backend - you should create a new test class
 * that extends this one providing hard implementations of {@link AspectDao} and implements
 * {@code @BeforeMethod} etc to set up and tear down state.
 *
 * <p>If you realise that a feature you want to test, sadly, has divergent behaviours between
 * different storage implementations, that you can't rectify - you should make the test method
 * abstract and implement it in all implementations of this class.
 *
 * @param <T_AD> {@link AspectDao} implementation.
 */
public abstract class TimelineServiceTest<T_AD extends AspectDao> {

  protected T_AD _aspectDao;

  protected final EntityRegistry _snapshotEntityRegistry = new TestEntityRegistry();
  protected final EntityRegistry _configEntityRegistry =
      new ConfigEntityRegistry(
          Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  protected final EntityRegistry _testEntityRegistry =
      new MergedEntityRegistry(_snapshotEntityRegistry).apply(_configEntityRegistry);
  protected TimelineServiceImpl _entityTimelineService;
  protected EntityServiceImpl _entityServiceImpl;
  protected EventProducer _mockProducer;
  protected UpdateIndicesService _mockUpdateIndicesService = mock(UpdateIndicesService.class);
  protected OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(_testEntityRegistry);

  protected TimelineServiceTest() throws EntityRegistryException {}

  @Test
  public void testGetTimeline() throws Exception {

    Urn entityUrn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:hive,fooDb.fooTable"
                + System.currentTimeMillis()
                + ",PROD)");
    String aspectName = "schemaMetadata";

    ArrayList<AuditStamp> timestamps = new ArrayList();
    // ingest records over time
    for (int i = 7; i > 0; i--) {
      // i days ago
      SchemaMetadata schemaMetadata = getSchemaMetadata("This is the new description for day " + i);
      AuditStamp daysAgo = createTestAuditStamp(i);
      timestamps.add(daysAgo);
      _entityServiceImpl.ingestAspects(
          opContext,
          entityUrn,
          Collections.singletonList(new Pair<>(aspectName, schemaMetadata)),
          daysAgo,
          getSystemMetadata(daysAgo, "run-" + i));
    }

    Map<String, RecordTemplate> latestAspects =
        _entityServiceImpl.getLatestAspectsForUrn(
            opContext, entityUrn, new HashSet<>(Arrays.asList(aspectName)), false);

    Set<ChangeCategory> elements = new HashSet<>();
    elements.add(ChangeCategory.TECHNICAL_SCHEMA);
    List<ChangeTransaction> changes =
        _entityTimelineService.getTimeline(
            entityUrn, elements, createTestAuditStamp(10).getTime(), 0, null, null, false);
    // Assert.assertEquals(changes.size(), 7);
    // Assert.assertEquals(changes.get(0).getChangeEvents().get(0).getChangeType(),
    // ChangeOperation.ADD);
    // Assert.assertEquals(changes.get(0).getTimestamp(), timestamps.get(0).getTime().longValue());
    // Assert.assertEquals(changes.get(1).getChangeEvents().get(0).getChangeType(),
    // ChangeOperation.MODIFY);
    // Assert.assertEquals(changes.get(1).getTimestamp(), timestamps.get(1).getTime().longValue());

    changes =
        _entityTimelineService.getTimeline(
            entityUrn, elements, timestamps.get(4).getTime() - 3000L, 0, null, null, false);
    // Assert.assertEquals(changes.size(), 3);
    // Assert.assertEquals(changes.get(0).getChangeEvents().get(0).getChangeType(),
    // ChangeOperation.MODIFY);
    // Assert.assertEquals(changes.get(0).getTimestamp(), timestamps.get(4).getTime().longValue());
    // Assert.assertEquals(changes.get(1).getChangeEvents().get(0).getChangeType(),
    // ChangeOperation.MODIFY);
    // Assert.assertEquals(changes.get(1).getTimestamp(), timestamps.get(5).getTime().longValue());
  }

  private static AuditStamp createTestAuditStamp(int daysAgo) {
    try {
      Long timestamp = System.currentTimeMillis() - (daysAgo * 24 * 60 * 60 * 1000L);
      Long timestampRounded = 1000 * (timestamp / 1000);
      return new AuditStamp()
          .setTime(timestampRounded)
          .setActor(Urn.createFromString("urn:li:principal:tester"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create urn");
    }
  }

  @Test
  public void testGetTimelineForDocument() throws Exception {
    // Test that Document entity is properly registered in TimelineServiceImpl
    Urn documentUrn =
        Urn.createFromString("urn:li:document:test-doc-" + System.currentTimeMillis());
    String aspectName = "documentInfo";

    ArrayList<AuditStamp> timestamps = new ArrayList();
    // Ingest document changes over time
    for (int i = 3; i > 0; i--) {
      com.linkedin.knowledge.DocumentInfo documentInfo =
          getDocumentInfo("Document version " + i, "Content for version " + i);
      AuditStamp daysAgo = createTestAuditStamp(i);
      timestamps.add(daysAgo);
      _entityServiceImpl.ingestAspects(
          opContext,
          documentUrn,
          Collections.singletonList(new Pair<>(aspectName, documentInfo)),
          daysAgo,
          getSystemMetadata(daysAgo, "run-" + i));
    }

    // Test getting timeline for DOCUMENTATION category
    Set<ChangeCategory> elements = new HashSet<>();
    elements.add(ChangeCategory.DOCUMENTATION);
    List<ChangeTransaction> changes =
        _entityTimelineService.getTimeline(
            documentUrn, elements, createTestAuditStamp(10).getTime(), 0, null, null, false);

    // Verify that timeline was generated for document
    // The first change should be creation, subsequent ones should be modifications
    // Note: We're just verifying the service can process Document entities,
    // detailed timeline logic is tested in DocumentInfoChangeEventGeneratorTest
    assert changes != null;
    assert !changes.isEmpty(); // Should have at least the creation event
  }

  private SystemMetadata getSystemMetadata(AuditStamp twoDaysAgo, String s) {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setLastObserved(twoDaysAgo.getTime());
    metadata1.setRunId(s);
    return metadata1;
  }

  private SchemaMetadata getSchemaMetadata(String s) {
    SchemaField field1 =
        new SchemaField()
            .setFieldPath("column1")
            .setDescription(s)
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("string");

    SchemaFieldArray fieldArray = new SchemaFieldArray();
    fieldArray.add(field1);

    return new SchemaMetadata()
        .setSchemaName("testSchema")
        .setPlatformSchema(
            SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema("foo")))
        .setPlatform(new DataPlatformUrn("hive"))
        .setHash("")
        .setVersion(0L)
        .setDataset(new DatasetUrn(new DataPlatformUrn("hive"), "testDataset", FabricType.TEST))
        .setFields(fieldArray);
  }

  private com.linkedin.knowledge.DocumentInfo getDocumentInfo(String title, String content) {
    com.linkedin.knowledge.DocumentInfo documentInfo = new com.linkedin.knowledge.DocumentInfo();
    documentInfo.setTitle(title);
    com.linkedin.knowledge.DocumentContents contents =
        new com.linkedin.knowledge.DocumentContents();
    contents.setText(content);
    documentInfo.setContents(contents);

    // Set required status field
    com.linkedin.knowledge.DocumentStatus status = new com.linkedin.knowledge.DocumentStatus();
    status.setState(com.linkedin.knowledge.DocumentState.PUBLISHED);
    documentInfo.setStatus(status);

    // Set created timestamp
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    try {
      created.setActor(Urn.createFromString("urn:li:corpuser:testUser"));
    } catch (Exception e) {
      // ignore
    }
    documentInfo.setCreated(created);
    documentInfo.setLastModified(created);

    return documentInfo;
  }
}
