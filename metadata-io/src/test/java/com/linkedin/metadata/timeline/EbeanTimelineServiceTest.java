package com.linkedin.metadata.timeline;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static org.mockito.Mockito.mock;

public class EbeanTimelineServiceTest extends TimelineServiceTestBase<EbeanAspectDao> {

  public EbeanTimelineServiceTest() throws EntityRegistryException {
  }

  @BeforeMethod
  public void setupTest() {
    EbeanServer server = EbeanServerFactory.create(createTestingH2ServerConfig());
    _aspectDao = new EbeanAspectDao(server);
    _aspectDao.setConnectionValidated(true);
    _entityTimelineService = new TimelineServiceImpl(_aspectDao);
    _mockProducer = mock(EventProducer.class);
    _entityService = new EntityService(_aspectDao, _mockProducer, _testEntityRegistry);
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

  /**
   * Ideally, all tests would be in the base class, so they're reused between all implementations.
   * When that's the case - test runner will ignore this class (and its base!) so we keep this dummy test
   * to make sure this class will always be discovered.
   */
  @Test
  public void obligatoryTest() throws Exception {
    Assert.assertTrue(true);
  }
}
