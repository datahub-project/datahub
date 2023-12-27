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
          entityUrn,
          Collections.singletonList(new Pair<>(aspectName, schemaMetadata)),
          daysAgo,
          getSystemMetadata(daysAgo, "run-" + i));
    }

    Map<String, RecordTemplate> latestAspects =
        _entityServiceImpl.getLatestAspectsForUrn(
            entityUrn, new HashSet<>(Arrays.asList(aspectName)));

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
}
