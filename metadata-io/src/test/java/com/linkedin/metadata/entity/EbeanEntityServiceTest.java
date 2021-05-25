package com.linkedin.metadata.entity;

import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;


public class EbeanEntityServiceTest {

  private EbeanServer _server;
  private KafkaMetadataEventProducer _mockProducer;

  @BeforeMethod
  public void setupTest() {
    _server = EbeanServerFactory.create(createTestingH2ServerConfig());
    _mockProducer = Mockito.mock(KafkaMetadataEventProducer.class);
  }


  @Nonnull
  public static ServerConfig createTestingH2ServerConfig() {
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
}
