package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.AspectIngestionUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.service.UpdateIndicesService;
import io.ebean.Database;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanAspectMigrationsDaoTest extends AspectMigrationsDaoTest<EbeanAspectDao> {

  private Database server;

  public EbeanAspectMigrationsDaoTest() {}

  @BeforeMethod
  public void setupTest() {
    server = EbeanTestUtils.createTestServer(EbeanAspectMigrationsDaoTest.class.getSimpleName());
    _mockProducer = mock(EventProducer.class);
    EbeanAspectDao dao =
        new EbeanAspectDao(server, EbeanConfiguration.testDefault, null, List.of(), null);
    dao.setConnectionValidated(true);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl = new EntityServiceImpl(dao, _mockProducer, true, preProcessHooks, true);
    _entityServiceImpl.setUpdateIndicesService(_mockUpdateIndicesService);
    _retentionService = new EbeanRetentionService(_entityServiceImpl, server, 1000);
    _entityServiceImpl.setRetentionService(_retentionService);

    _migrationsDao = dao;
  }

  @Test
  public void testStreamAspects() throws AssertionError {
    final int totalAspects = 30;
    Map<Urn, CorpUserKey> ingestedAspects =
        AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, totalAspects);
    List<String> ingestedUrns =
        ingestedAspects.keySet().stream().map(Urn::toString).collect(Collectors.toList());

    List<EntityAspect> aspectList;
    try (Stream<EntityAspect> stream =
        _migrationsDao.streamAspects(CORP_USER_ENTITY_NAME, CORP_USER_KEY_ASPECT_NAME)) {
      aspectList = stream.collect(Collectors.toList());
    }

    assertEquals(ingestedUrns.size(), aspectList.size());
    Set<String> urnsFetched =
        aspectList.stream().map(EntityAspect::getUrn).collect(Collectors.toSet());
    for (String urn : ingestedUrns) {
      assertTrue(urnsFetched.contains(urn));
    }
  }

  // ── streamAspectBatchesForMigration ───────────────────────────────────────

  @Test
  public void testStreamAspectBatchesForMigration_emptyTargetMap() {
    AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, 3);

    List<EbeanAspectV2> result;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(Map.of(), 0, 100, 0)) {
      result = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    assertTrue(result.isEmpty());
  }

  @Test
  public void testStreamAspectBatchesForMigration_allAspectsAtDefaultVersion() {
    // DEFAULT_SCHEMA_VERSION = 1L; target=1L means versionedAspects is empty → early return
    AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, 3);

    List<EbeanAspectV2> result;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(
            Map.of(CORP_USER_KEY_ASPECT_NAME, 1L), 0, 100, 0)) {
      result = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    assertTrue(result.isEmpty());
  }

  @Test
  public void testStreamAspectBatchesForMigration_returnsUnmigratedAspects() {
    final int count = 5;
    AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, count);

    List<EbeanAspectV2> result;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(
            Map.of(CORP_USER_KEY_ASPECT_NAME, 2L), 0, 100, 0)) {
      result = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    assertEquals(result.size(), count);
  }

  @Test
  public void testStreamAspectBatchesForMigration_excludesMigratedAspects() {
    // 3 unmigrated aspects (schemaVersion=1, target=2)
    AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, 3);

    // 2 already-migrated aspects inserted directly with schemaVersion=2
    for (int i = 0; i < 2; i++) {
      EbeanAspectV2 migrated =
          new EbeanAspectV2(
              "urn:li:corpuser:migrated" + i,
              CORP_USER_KEY_ASPECT_NAME,
              0L,
              "{\"username\":\"migrated" + i + "\"}",
              new Timestamp(System.currentTimeMillis() + i),
              "urn:li:corpuser:datahub",
              null,
              "{\"schemaVersion\":2,\"runId\":\"test\"}");
      server.save(migrated);
    }

    List<EbeanAspectV2> result;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(
            Map.of(CORP_USER_KEY_ASPECT_NAME, 2L), 0, 100, 0)) {
      result = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    // Only the 3 unmigrated aspects are returned; the 2 at target version are excluded
    assertEquals(result.size(), 3);
  }

  @Test
  public void testStreamAspectBatchesForMigration_afterCreatedOnMsFilter() {
    long earlyMs = 1_000L;
    long lateMs = 3_000L;

    EbeanAspectV2 early =
        new EbeanAspectV2(
            "urn:li:corpuser:early0",
            CORP_USER_KEY_ASPECT_NAME,
            0L,
            "{\"username\":\"early0\"}",
            new Timestamp(earlyMs),
            "urn:li:corpuser:datahub",
            null,
            "{\"schemaVersion\":1,\"runId\":\"test\"}");
    server.save(early);

    EbeanAspectV2 late =
        new EbeanAspectV2(
            "urn:li:corpuser:late0",
            CORP_USER_KEY_ASPECT_NAME,
            0L,
            "{\"username\":\"late0\"}",
            new Timestamp(lateMs),
            "urn:li:corpuser:datahub",
            null,
            "{\"schemaVersion\":1,\"runId\":\"test\"}");
    server.save(late);

    // No timestamp filter → both returned
    List<EbeanAspectV2> allResults;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(
            Map.of(CORP_USER_KEY_ASPECT_NAME, 2L), 0, 100, 0)) {
      allResults = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    assertEquals(allResults.size(), 2);

    // Filter to after earlyMs → only late returned
    List<EbeanAspectV2> filteredResults;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(
            Map.of(CORP_USER_KEY_ASPECT_NAME, 2L), earlyMs + 1, 100, 0)) {
      filteredResults = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    assertEquals(filteredResults.size(), 1);
    assertEquals(filteredResults.get(0).getUrn(), "urn:li:corpuser:late0");
  }

  @Test
  public void testStreamAspectBatchesForMigration_limitApplied() {
    for (int i = 0; i < 5; i++) {
      EbeanAspectV2 a =
          new EbeanAspectV2(
              "urn:li:corpuser:limitTest" + i,
              CORP_USER_KEY_ASPECT_NAME,
              0L,
              "{\"username\":\"limitTest" + i + "\"}",
              new Timestamp(1000L + i),
              "urn:li:corpuser:datahub",
              null,
              "{\"schemaVersion\":1,\"runId\":\"test\"}");
      server.save(a);
    }

    List<EbeanAspectV2> limited;
    try (PartitionedStream<EbeanAspectV2> stream =
        _migrationsDao.streamAspectBatchesForMigration(
            Map.of(CORP_USER_KEY_ASPECT_NAME, 2L), 0, 100, 2)) {
      limited = stream.partition(100).flatMap(s -> s).collect(Collectors.toList());
    }
    assertEquals(limited.size(), 2);
  }

  @AfterMethod
  public void cleanup() {
    // Shutdown Database instance to prevent thread pool and connection leaks
    // This includes the "gma.heartBeat" thread and connection pools
    EbeanTestUtils.shutdownDatabase(server);
  }
}
