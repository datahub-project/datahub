package io.datahubproject.openapi.scim;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static org.mockito.Mockito.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import com.linkedin.metadata.key.DataHubRoleKey;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.util.Neo4jTestServerBuilder;
import io.ebean.Database;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@SpringBootApplication(
    scanBasePackages = {
      "io.datahubproject.openapi",
      "org.springdoc.webmvc.ui",
      "org.springdoc.core",
      "org.springdoc.webmvc.core",
      "org.springframework.boot.autoconfigure.jackson",
      "com.linkedin.gms.factory.scim",
      "io.datahubproject.openapi.scim"
    })
@Import({ConfigurationProvider.class})
public class ScimpleSpringTestApp {
  // Static instance to ensure only one container across all Spring contexts
  private static volatile Neo4jTestServerBuilder SHARED_NEO4J_BUILDER;
  private static final Object NEO4J_LOCK = new Object();

  @Value("${secretService.encryptionKey}")
  private String encryptionKey;

  // Store the server builder as a field for cleanup
  private Neo4jTestServerBuilder neo4jServerBuilder;

  @Bean("systemOperationContext")
  @ConditionalOnMissingBean
  OperationContext systemOperationContext() {
    return TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Bean(name = "dataHubSecretService")
  @ConditionalOnMissingBean
  @Primary
  @Nonnull
  protected SecretService getInstance(final ConfigurationProvider configurationProvider) {
    return new SecretService(
        this.encryptionKey, configurationProvider.getSecretService().isV1AlgorithmEnabled());
  }

  @Bean
  @ConditionalOnMissingBean
  Database server() {
    return EbeanTestUtils.createTestServer("apache-scimple-test-cases");
  }

  @Bean
  @ConditionalOnMissingBean
  EntityService<?> entityService(
      Database server,
      GraphService graphService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext)
      throws EntityRegistryException, IOException, URISyntaxException {

    EbeanAspectDao _aspectDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault, null);

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            UpdateGraphIndicesService.withService(graphService),
            mock(ElasticSearchService.class),
            mock(SystemMetadataService.class),
            Collections.emptyList(),
            false,
            false,
            false);

    EventProducer _mockProducer = mock(EventProducer.class);
    when(_mockProducer.produceMetadataChangeLog(any(OperationContext.class), any(), any(), any()))
        .thenAnswer(
            x -> {
              MetadataChangeLog mcl = x.getArgument(3);
              updateIndicesService.handleChangeEvent(systemOperationContext, mcl);
              return null;
            });

    PreProcessHooks preProcessHooks = new PreProcessHooks();

    preProcessHooks.setUiEnabled(true);

    EntityServiceImpl _entityServiceImpl =
        new EntityServiceImpl(_aspectDao, _mockProducer, true, preProcessHooks, true);

    _entityServiceImpl.setUpdateIndicesService(updateIndicesService);

    EbeanRetentionService _retentionService =
        new EbeanRetentionService(_entityServiceImpl, server, 1000);

    _entityServiceImpl.setRetentionService(_retentionService);

    _entityServiceImpl.setUpdateIndicesService(updateIndicesService);

    ingestSystemRoles(systemOperationContext, _entityServiceImpl);
    try {
      System.out.println(
          _entityServiceImpl.getLatestAspects(
              systemOperationContext,
              ImmutableSet.of(
                  Urn.createFromString("urn:li:dataHubRole:Admin"),
                  Urn.createFromString("urn:li:dataHubRole:Editor"),
                  Urn.createFromString("urn:li:dataHubRole:Reader")),
              ImmutableSet.of(DATAHUB_ROLE_INFO_ASPECT_NAME, "dataHubRoleKey")));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return _entityServiceImpl;
  }

  private static void ingestSystemRoles(
      OperationContext opContext, EntityServiceImpl _entityServiceImpl)
      throws IOException, URISyntaxException {
    final ObjectMapper mapper = new YAMLMapper();
    final JsonNode mcpsObj =
        mapper.readTree(
            ScimpleSpringTestApp.class
                .getClassLoader()
                .getResourceAsStream("bootstrap_mcps/roles.yaml"));
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    for (final JsonNode mcpObj : mcpsObj) {
      Urn roleUrn = Urn.createFromString(mcpObj.get("entityUrn").asText());

      DataHubRoleKey roleKey = new DataHubRoleKey();
      roleKey.setId(mcpObj.get("aspect").get("name").toString());

      DataHubRoleInfo roleInfo =
          RecordUtils.toRecordTemplate(DataHubRoleInfo.class, mcpObj.get("aspect").toString());

      MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
      keyAspectProposal.setEntityUrn(roleUrn);
      keyAspectProposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
      keyAspectProposal.setAspectName("dataHubRoleKey");
      keyAspectProposal.setAspect(GenericRecordUtils.serializeAspect(roleKey));
      keyAspectProposal.setChangeType(ChangeType.UPSERT);
      _entityServiceImpl.ingestProposal(opContext, keyAspectProposal, auditStamp, false);

      MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(roleUrn);
      proposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
      proposal.setAspectName(DATAHUB_ROLE_INFO_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(roleInfo));
      proposal.setChangeType(ChangeType.UPSERT);
      _entityServiceImpl.ingestProposal(opContext, proposal, auditStamp, false);
    }
  }

  @Bean
  @ConditionalOnMissingBean
  GraphService graphService(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {

    // Ensure only one Neo4j container is created across all test contexts
    Neo4jTestServerBuilder builder;
    synchronized (NEO4J_LOCK) {
      if (SHARED_NEO4J_BUILDER == null) {
        SHARED_NEO4J_BUILDER = new Neo4jTestServerBuilder();
        SHARED_NEO4J_BUILDER.start();

        // Register shutdown hook to clean up when JVM exits
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      if (SHARED_NEO4J_BUILDER != null) {
                        SHARED_NEO4J_BUILDER.shutdown();
                      }
                    }));
      }
      builder = SHARED_NEO4J_BUILDER;
    }

    // Get the driver from the shared builder
    Driver _driver = builder.getDriver();

    // Clear the database for test isolation
    try (var session = _driver.session()) {
      session.run("MATCH (n) DETACH DELETE n").consume();
    }

    // Create the Neo4j graph service
    Neo4jGraphService _client =
        new Neo4jGraphService(
            new LineageRegistry(SnapshotEntityRegistry.getInstance()),
            _driver,
            SessionConfig.defaultConfig(),
            TEST_GRAPH_SERVICE_CONFIG);

    return _client;
  }
}
