package com.linkedin.metadata.entity.storage;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testcontainers integration test for split-pool read routing (same PostgreSQL instance, separate
 * Ebean pools). The READ pool uses JDBC read-only connections.
 */
public class PrimaryStorageReadPoolPostgresIT {

  private PostgreSQLContainer<?> postgres;
  private Database primaryDatabase;
  private Database readPoolDatabase;
  private PrimaryStorageResolver resolver;
  private EbeanAspectDao aspectDao;
  private OperationContext opContext;

  @BeforeClass
  public void init() throws SQLException {
    postgres = PostgresTestUtils.startPostgres();
    primaryDatabase =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres, PostgresTestUtils.uniqueServerName("readpool_it_primary"));
    readPoolDatabase =
        PostgresTestUtils.createEbeanReadPoolDatabase(
            postgres, PostgresTestUtils.uniqueServerName("readpool_it_read"));

    resolver = PrimaryStorageTestUtils.splitPoolEbeanResolver(primaryDatabase, readPoolDatabase);
    aspectDao = new EbeanAspectDao(resolver, EbeanConfiguration.testDefault, null, List.of(), null);
    aspectDao.setConnectionValidated(true);

    opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(readPoolDatabase);
    EbeanTestUtils.shutdownDatabase(primaryDatabase);
  }

  @Test
  public void splitPool_registersReadTargetAndRoutesReads() {
    assertTrue(resolver.getRegistry().has(io.datahubproject.metadata.context.StorageTarget.READ));
    assertNotSame(primaryDatabase, readPoolDatabase);
    assertSame(readPoolDatabase, resolver.resolveEbean(opContext, false));
    assertSame(primaryDatabase, resolver.resolveEbean(opContext, true));
    assertSame(primaryDatabase, resolver.resolveEbean(null, false));
  }

  @Test
  public void readPoolConnectionsAreJdbcReadOnly() throws SQLException {
    try (Connection connection = readPoolDatabase.dataSource().getConnection()) {
      assertTrue(connection.isReadOnly(), "READ pool JDBC connections must be read-only");
    }
  }

  @Test
  public void readPoolRejectsDirectWrites_primaryPoolAllowsWrites() throws Exception {
    String urn = "urn:li:corpuser:readpool_it_user";
    EbeanAspectV2 row = new EbeanAspectV2();
    row.setUrn(urn);
    row.setAspect(STATUS_ASPECT_NAME);
    row.setVersion(ASPECT_LATEST_VERSION);
    row.setMetadata("{\"removed\":false}");
    row.setCreatedBy("urn:li:corpuser:test");
    row.setCreatedOn(new Timestamp(System.currentTimeMillis()));

    try {
      readPoolDatabase.save(row);
      fail("Expected write via READ pool to fail");
    } catch (Exception expected) {
      // JDBC read-only connection rejects mutations
    }

    primaryDatabase.save(row);
    EntityAspect readBack =
        aspectDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertNotNull(readBack);
    assertEquals(readBack.getUrn(), urn);
  }

  @Test
  public void aspectDaoReadsViaReadPool() {
    String urn = "urn:li:corpuser:readpool_batch";
    EbeanAspectV2 row = new EbeanAspectV2();
    row.setUrn(urn);
    row.setAspect(STATUS_ASPECT_NAME);
    row.setVersion(ASPECT_LATEST_VERSION);
    row.setMetadata("{\"removed\":false}");
    row.setCreatedBy("urn:li:corpuser:test");
    row.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    primaryDatabase.save(row);

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    Map<EntityAspectIdentifier, EntityAspect> batch =
        aspectDao.batchGet(opContext, Set.of(key), false);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(key).getUrn(), urn);
    assertEquals(aspectDao.getServer(), primaryDatabase);
  }

  @Test
  public void getNextVersionsWritePathUsesPrimary() {
    String urn = "urn:li:corpuser:readpool_next_version";
    EbeanAspectV2 row = new EbeanAspectV2();
    row.setUrn(urn);
    row.setAspect(STATUS_ASPECT_NAME);
    row.setVersion(ASPECT_LATEST_VERSION);
    row.setMetadata("{\"removed\":false}");
    row.setCreatedBy("urn:li:corpuser:test");
    row.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    primaryDatabase.save(row);

    Map<String, Map<String, Long>> nextVersions =
        aspectDao.getNextVersions(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true);

    assertEquals(nextVersions.get(urn).get(STATUS_ASPECT_NAME).longValue(), 1L);
  }
}
