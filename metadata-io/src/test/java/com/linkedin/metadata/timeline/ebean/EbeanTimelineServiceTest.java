package com.linkedin.metadata.timeline.ebean;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
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
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class EbeanTimelineServiceTest {

  private static final AuditStamp TEST_AUDIT_STAMP = createTestAuditStamp(1);
  private final EntityRegistry _snapshotEntityRegistry = new TestEntityRegistry();
  private final EntityRegistry _configEntityRegistry =
      new ConfigEntityRegistry(Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  private final EntityRegistry _testEntityRegistry =
      new MergedEntityRegistry(_snapshotEntityRegistry).apply(_configEntityRegistry);
  private EbeanAspectDao _aspectDao;
  private EbeanServer _server;
  private EbeanTimelineService _entityTimelineService;
  private EbeanEntityService _entityService;
  private EventProducer _mockProducer;

  public EbeanTimelineServiceTest() throws EntityRegistryException {
  }

  @Nonnull
  private static ServerConfig createTestingH2ServerConfig() {

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    boolean usingH2 = true;

    if (usingH2) {
      dataSourceConfig.setUsername("tester");
      dataSourceConfig.setPassword("");
      dataSourceConfig.setUrl("jdbc:h2:mem:;IGNORECASE=TRUE;");
      dataSourceConfig.setDriver("org.h2.Driver");
      //dataSourceConfig.setIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ);
    } else {
      dataSourceConfig.setUsername("datahub");
      dataSourceConfig.setPassword("datahub");
      dataSourceConfig.setUrl("jdbc:mysql://localhost:3306/datahub?verifyServerCertificate=false&useSSL"
          + "=true&useUnicode=yes&characterEncoding=UTF-8&enabledTLSProtocols=TLSv1.2");
      dataSourceConfig.setDriver("com.mysql.jdbc.Driver");
      dataSourceConfig.setMinConnections(1);
      dataSourceConfig.setMaxConnections(10);
    }

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    if (usingH2) {
      serverConfig.setDdlGenerate(true);
      serverConfig.setDdlRun(true);
    } else {
      serverConfig.setDdlGenerate(false);
      serverConfig.setDdlRun(false);
    }

    return serverConfig;
  }

  private static AuditStamp createTestAuditStamp(int daysAgo) {
    try {
      Long timestamp = System.currentTimeMillis() - (daysAgo * 24 * 60 * 60 * 1000L);
      Long timestampRounded = 1000 * (timestamp / 1000);
      return new AuditStamp().setTime(timestampRounded).setActor(Urn.createFromString("urn:li:principal:tester"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create urn");
    }
  }

  @BeforeMethod
  public void setupTest() {
    _server = EbeanServerFactory.create(createTestingH2ServerConfig());
    _aspectDao = new EbeanAspectDao(_server);
    _aspectDao.setConnectionValidated(true);
    _entityTimelineService = new EbeanTimelineService(_aspectDao);
    _mockProducer = mock(EventProducer.class);
    _entityService = new EbeanEntityService(_aspectDao, _mockProducer, _testEntityRegistry);
  }

  @Test
  public void testGetTimeline() throws Exception {

    Urn entityUrn = Urn.createFromString(
        "urn:li:dataset:(urn:li:dataPlatform:hive,fooDb.fooTable" + System.currentTimeMillis() + ",PROD)");
    String aspectName = "schemaMetadata";

    ArrayList<AuditStamp> timestamps = new ArrayList();
    // ingest records over time
    for (int i = 7; i > 0; i--) {
      // i days ago
      SchemaMetadata schemaMetadata = getSchemaMetadata("This is the new description for day " + i);
      AuditStamp daysAgo = createTestAuditStamp(i);
      timestamps.add(daysAgo);
      _entityService.ingestAspects(entityUrn, Collections.singletonList(new Pair<>(aspectName, schemaMetadata)),
          daysAgo, getSystemMetadata(daysAgo, "run-" + i));
    }

    Map<String, RecordTemplate> latestAspects =
        _entityService.getLatestAspectsForUrn(entityUrn, new HashSet<>(Arrays.asList(aspectName)));

    Set<ChangeCategory> elements = new HashSet<>();
    elements.add(ChangeCategory.TECHNICAL_SCHEMA);
    List<ChangeTransaction> changes =
        _entityTimelineService.getTimeline(entityUrn, elements, createTestAuditStamp(10).getTime(), 0, null, null,
            false);
    //Assert.assertEquals(changes.size(), 7);
    //Assert.assertEquals(changes.get(0).getChangeEvents().get(0).getChangeType(), ChangeOperation.ADD);
    //Assert.assertEquals(changes.get(0).getTimestamp(), timestamps.get(0).getTime().longValue());
    //Assert.assertEquals(changes.get(1).getChangeEvents().get(0).getChangeType(), ChangeOperation.MODIFY);
    //Assert.assertEquals(changes.get(1).getTimestamp(), timestamps.get(1).getTime().longValue());

    changes =
        _entityTimelineService.getTimeline(entityUrn, elements, timestamps.get(4).getTime() - 3000L, 0, null, null,
            false);
    //Assert.assertEquals(changes.size(), 3);
    //Assert.assertEquals(changes.get(0).getChangeEvents().get(0).getChangeType(), ChangeOperation.MODIFY);
    //Assert.assertEquals(changes.get(0).getTimestamp(), timestamps.get(4).getTime().longValue());
    //Assert.assertEquals(changes.get(1).getChangeEvents().get(0).getChangeType(), ChangeOperation.MODIFY);
    //Assert.assertEquals(changes.get(1).getTimestamp(), timestamps.get(5).getTime().longValue());
  }

  private SystemMetadata getSystemMetadata(AuditStamp twoDaysAgo, String s) {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setLastObserved(twoDaysAgo.getTime());
    metadata1.setRunId(s);
    return metadata1;
  }

  private SchemaMetadata getSchemaMetadata(String s) {
    SchemaField field1 = new SchemaField()
        .setFieldPath("column1")
        .setDescription(s)
        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))
        .setNativeDataType("string");

    SchemaFieldArray fieldArray = new SchemaFieldArray();
    fieldArray.add(field1);

    return new SchemaMetadata().setSchemaName("testSchema")
        .setPlatformSchema(SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema("foo")))
        .setPlatform(new DataPlatformUrn("hive"))
        .setHash("")
        .setVersion(0L)
        .setDataset(new DatasetUrn(new DataPlatformUrn("hive"), "testDataset", FabricType.TEST))
        .setFields(fieldArray);
  }
}
