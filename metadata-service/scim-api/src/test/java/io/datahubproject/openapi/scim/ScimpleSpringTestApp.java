package io.datahubproject.openapi.scim;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
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
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.util.EbeanTestUtils;
import io.datahubproject.test.util.Neo4jTestServerBuilder;
import io.datahubproject.test.util.TestEntityRegistry;
import io.ebean.Database;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.directory.scim.core.repository.DefaultPatchHandler;
import org.apache.directory.scim.core.repository.PatchHandler;
import org.apache.directory.scim.core.repository.Repository;
import org.apache.directory.scim.core.repository.RepositoryRegistry;
import org.apache.directory.scim.core.schema.SchemaRegistry;
import org.apache.directory.scim.server.configuration.ServerConfiguration;
import org.apache.directory.scim.server.rest.EtagGenerator;
import org.apache.directory.scim.spec.resources.ScimResource;
import org.apache.directory.scim.spec.schema.ServiceProviderConfiguration;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;

@SpringBootApplication(
    scanBasePackages = {
      "io.datahubproject.openapi",
      "org.apache.directory.scim.server",
      "org.springdoc.webmvc.ui",
      "org.springdoc.core",
      "org.springdoc.webmvc.core",
      "org.springframework.boot.autoconfigure.jackson",
      "io.datahubproject.openapi.scim"
    })
public class ScimpleSpringTestApp {
  @Value("${secretService.encryptionKey}")
  private String encryptionKey;

  @Bean
  ServerConfiguration serverConfiguration() {
    return new ServerConfiguration()
        .addAuthenticationSchema(ServiceProviderConfiguration.AuthenticationSchema.oauthBearer());
  }

  @Bean
  @ConditionalOnMissingBean
  SchemaRegistry schemaRegistry() {
    return new SchemaRegistry();
  }

  @Bean
  @ConditionalOnMissingBean
  RepositoryRegistry repositoryRegistry(
      SchemaRegistry schemaRegistry, List<Repository<? extends ScimResource>> scimResources) {

    RepositoryRegistry registry = new RepositoryRegistry(schemaRegistry);

    registry.registerRepositories(scimResources);

    return registry;
  }

  @Bean
  @ConditionalOnMissingBean
  EtagGenerator etagGenerator() {
    return new EtagGenerator();
  }

  @Bean
  @ConditionalOnMissingBean
  PatchHandler patchHandler(SchemaRegistry schemaRegistry) {
    return new DefaultPatchHandler(schemaRegistry);
  }

  @Bean(name = "dataHubSecretService")
  @ConditionalOnMissingBean
  @Primary
  @Nonnull
  protected SecretService getInstance() {
    return new SecretService(this.encryptionKey);
  }

  @Bean
  @ConditionalOnMissingBean
  Database server() {
    return EbeanTestUtils.createTestServer("apache-scimple-test-cases");
  }

  @Bean
  @ConditionalOnMissingBean
  EntityService entityService(Database server, GraphService graphService)
      throws EntityRegistryException, IOException, URISyntaxException {

    EbeanAspectDao _aspectDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault);

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            graphService,
            mock(EntitySearchService.class),
            mock(TimeseriesAspectService.class),
            mock(SystemMetadataService.class),
            mock(SearchDocumentTransformer.class),
            mock(EntityIndexBuilders.class));

    EventProducer _mockProducer = mock(EventProducer.class);
    when(_mockProducer.produceMetadataChangeLog(any(), any(), any()))
        .thenAnswer(
            x -> {
              MetadataChangeLog mcl = x.getArgument(2);
              updateIndicesService.handleChangeEvent(mcl);
              return null;
            });

    PreProcessHooks preProcessHooks = new PreProcessHooks();

    preProcessHooks.setUiEnabled(true);

    EntityRegistry _configEntityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));

    EntityRegistry _snapshotEntityRegistry = new TestEntityRegistry();

    EntityRegistry _testEntityRegistry =
        new MergedEntityRegistry(_snapshotEntityRegistry).apply(_configEntityRegistry);

    EntityServiceImpl _entityServiceImpl =
        new EntityServiceImpl(
            _aspectDao, _mockProducer, _testEntityRegistry, true, preProcessHooks, true);

    _entityServiceImpl.setUpdateIndicesService(updateIndicesService);

    EbeanRetentionService _retentionService =
        new EbeanRetentionService(_entityServiceImpl, server, 1000);

    _entityServiceImpl.setRetentionService(_retentionService);

    _entityServiceImpl.setUpdateIndicesService(updateIndicesService);

    ingestSystemRoles(_entityServiceImpl);
    try {
      System.out.println(
          _entityServiceImpl.getLatestAspects(
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

  private static void ingestSystemRoles(EntityServiceImpl _entityServiceImpl)
      throws IOException, URISyntaxException {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode rolesObj = mapper.readTree(new ClassPathResource("./roles.json").getFile());
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    for (final JsonNode roleObj : rolesObj) {
      Urn roleUrn = Urn.createFromString(roleObj.get("urn").asText());

      DataHubRoleKey roleKey = new DataHubRoleKey();
      roleKey.setId(roleObj.get("info").get("name").toString());

      DataHubRoleInfo roleInfo =
          RecordUtils.toRecordTemplate(DataHubRoleInfo.class, roleObj.get("info").toString());

      MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
      keyAspectProposal.setEntityUrn(roleUrn);
      keyAspectProposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
      keyAspectProposal.setAspectName("dataHubRoleKey");
      keyAspectProposal.setAspect(GenericRecordUtils.serializeAspect(roleKey));
      keyAspectProposal.setChangeType(ChangeType.UPSERT);
      _entityServiceImpl.ingestProposal(keyAspectProposal, auditStamp, false);

      MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(roleUrn);
      proposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
      proposal.setAspectName(DATAHUB_ROLE_INFO_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(roleInfo));
      proposal.setChangeType(ChangeType.UPSERT);
      _entityServiceImpl.ingestProposal(proposal, auditStamp, false);
    }
  }

  @Bean
  @ConditionalOnMissingBean
  GraphService graphService() {
    Neo4jTestServerBuilder _serverBuilder = new Neo4jTestServerBuilder();

    _serverBuilder.newServer();

    Driver _driver = GraphDatabase.driver(_serverBuilder.boltURI());

    Neo4jGraphService _client =
        new Neo4jGraphService(new LineageRegistry(SnapshotEntityRegistry.getInstance()), _driver);

    _client.clear();

    return _client;
  }
}
