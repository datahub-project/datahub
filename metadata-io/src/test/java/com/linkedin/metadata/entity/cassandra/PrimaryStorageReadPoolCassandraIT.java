package com.linkedin.metadata.entity.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.linkedin.metadata.CassandraTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testcontainers.cassandra.CassandraContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Testcontainers integration test for split-pool Cassandra read routing (same cluster, separate
 * sessions).
 */
public class PrimaryStorageReadPoolCassandraIT {

  private CassandraContainer cassandraContainer;
  private CqlSession primarySession;
  private CqlSession readPoolSession;
  private PrimaryStorageResolver resolver;
  private CassandraAspectDao aspectDao;
  private OperationContext opContext;

  @BeforeClass
  public void beforeClass() {
    cassandraContainer = CassandraTestUtils.setupContainer();
  }

  @BeforeMethod
  public void setupTest() {
    CassandraTestUtils.purgeData(cassandraContainer);
    primarySession = CassandraTestUtils.createTestSession(cassandraContainer);
    readPoolSession = CassandraTestUtils.createTestSession(cassandraContainer);
    resolver = PrimaryStorageTestUtils.splitPoolCassandraResolver(primarySession, readPoolSession);
    aspectDao = new CassandraAspectDao(resolver, List.of(), null);
    aspectDao.setConnectionValidated(true);
    opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);
  }

  @AfterMethod
  public void cleanup() {
    CassandraTestUtils.closeSession(readPoolSession);
    CassandraTestUtils.closeSession(primarySession);
    readPoolSession = null;
    primarySession = null;
  }

  @AfterClass
  public void tearDown() {
    cassandraContainer.stop();
  }

  @Test
  public void splitPool_registersReadTargetAndRoutesReads() {
    assertTrue(resolver.getRegistry().has(io.datahubproject.metadata.context.StorageTarget.READ));
    assertNotSame(primarySession, readPoolSession);
    assertSame(readPoolSession, resolver.resolveCassandra(opContext, false));
    assertSame(primarySession, resolver.resolveCassandra(opContext, true));
  }

  @Test
  public void aspectDaoReadsViaReadPool() {
    String urn = "urn:li:corpuser:readpool_cassandra_batch";
    String insertCql =
        QueryBuilder.insertInto(CassandraAspect.TABLE_NAME)
            .value(CassandraAspect.URN_COLUMN, literal(urn))
            .value(CassandraAspect.ASPECT_COLUMN, literal(STATUS_ASPECT_NAME))
            .value(CassandraAspect.VERSION_COLUMN, literal(ASPECT_LATEST_VERSION))
            .value(CassandraAspect.METADATA_COLUMN, literal("{\"removed\":false}"))
            .value(CassandraAspect.CREATED_BY_COLUMN, literal("urn:li:corpuser:test"))
            .value(CassandraAspect.CREATED_ON_COLUMN, literal(System.currentTimeMillis()))
            .build()
            .getQuery();
    primarySession.execute(SimpleStatement.newInstance(insertCql));

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    Map<EntityAspectIdentifier, EntityAspect> batch =
        aspectDao.batchGet(opContext, Set.of(key), false);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(key).getUrn(), urn);
  }
}
