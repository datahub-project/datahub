package com.linkedin.metadata.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.Transaction;
import io.ebean.TxScope;
import io.ebean.annotation.TxIsolation;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static org.mockito.Mockito.mock;


public class EbeanEntityServiceTest extends EntityServiceTestBase<EbeanAspectDao, EbeanRetentionService> {

  public EbeanEntityServiceTest() throws EntityRegistryException {
  }

  @Nonnull
  private static ServerConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:;IGNORECASE=TRUE;");
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }

  @BeforeMethod
  public void setupTest() {
    EbeanServer server = EbeanServerFactory.create(createTestingH2ServerConfig());
    _mockProducer = mock(EventProducer.class);
    _aspectDao = new EbeanAspectDao(server);
    _aspectDao.setConnectionValidated(true);
    _entityService = new EntityService(_aspectDao, _mockProducer, _testEntityRegistry);
    _retentionService = new EbeanRetentionService(_entityService, server, 1000);
    _entityService.setRetentionService(_retentionService);
  }

  @Test
  public void obligatoryTest() throws Exception {
    // We need this method to make test framework pick this class up.
    // All real tests are in the base class.
    Assert.assertTrue(true);
  }

  @Test
  public void testNestedTransactions() throws Exception {
    EbeanServer server = _aspectDao.getServer();

    try (Transaction transaction = server.beginTransaction(TxScope.requiresNew()
        .setIsolation(TxIsolation.REPEATABLE_READ))) {
      transaction.setBatchMode(true);
      // Work 1
      try (Transaction transaction2 = server.beginTransaction(TxScope.requiresNew()
          .setIsolation(TxIsolation.REPEATABLE_READ))) {
        transaction2.setBatchMode(true);
        // Work 2
        transaction2.commit();
      }
      transaction.commit();
    } catch (Exception e) {
      System.out.printf("Top level catch %s%n", e);
      e.printStackTrace();
      throw e;
    }
    System.out.println("done");
  }
}
