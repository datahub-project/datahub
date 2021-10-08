package com.linkedin.metadata.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.event.EntityEventProducer;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import org.testng.annotations.BeforeMethod;

import javax.annotation.Nonnull;
import static org.mockito.Mockito.mock;

public class EbeanEntityServiceTest extends EntityServiceTestBase {

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
    EbeanServer ebeanServer = EbeanServerFactory.create(createTestingH2ServerConfig());
    _mockProducer = mock(EntityEventProducer.class);
    EbeanAspectDao aspectDao = new EbeanAspectDao(ebeanServer);
    aspectDao.setConnectionValidated(true);
    _entityService = new EbeanEntityService(aspectDao, _mockProducer, _testEntityRegistry);
    _aspectDao = aspectDao;
  }
}
