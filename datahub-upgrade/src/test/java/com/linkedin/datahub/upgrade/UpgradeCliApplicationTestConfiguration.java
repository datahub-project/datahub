package com.linkedin.datahub.upgrade;

import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import io.ebean.Database;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class UpgradeCliApplicationTestConfiguration {

    @MockBean
    private UpgradeCli upgradeCli;

    @MockBean(name = "ebeanServer")
    public Database ebeanServer;

    @MockBean(name = "ebeanDatabaseConfig")
    public DatabaseConfig ebeanDatabaseConfig;

    @MockBean(name = "ebeanDataSourceConfig")
    public DataSourceConfig ebeanDataSourceConfig;

    @MockBean(name = "ebeanPrimaryServer")
    public Database ebeanPrimaryServer;

    @MockBean(name = "ebeanPrimaryDatabaseConfig")
    public DatabaseConfig ebeanPrimaryDatabaseConfig;

    @MockBean(name = "ebeanPrimaryDataSourceConfig")
    public DataSourceConfig ebeanPrimaryDataSourceConfig;

    @MockBean
    private EntityService entityService;

    @MockBean
    private SearchService searchService;

    @MockBean
    private GraphService graphService;

    @MockBean
    private EntityRegistry entityRegistry;

    @MockBean
    ConfigEntityRegistry configEntityRegistry;
}
