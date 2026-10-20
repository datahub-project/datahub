package com.linkedin.datahub.graphql.resolvers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProcessInstanceResult;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityAssertionsResult;
import com.linkedin.datahub.graphql.generated.EntityIncidentsResult;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.datahub.graphql.generated.RelatedDocumentsResult;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.resolvers.assertion.EntityAssertionsResolver;
import com.linkedin.datahub.graphql.resolvers.browse.EntityBrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.container.ParentContainersResolver;
import com.linkedin.datahub.graphql.resolvers.datacontract.EntityDataContractResolver;
import com.linkedin.datahub.graphql.resolvers.health.EntityHealthResolver;
import com.linkedin.datahub.graphql.resolvers.incident.EntityIncidentsResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.EntityRunsResolver;
import com.linkedin.datahub.graphql.resolvers.knowledge.RelatedDocumentsResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityLineageResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.siblings.SiblingsSearchResolver;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Fields resolved on an entity the caller can't view must answer like they do for an entity that
 * doesn't exist, without reading anything about it.
 */
public class UnviewableSourceResolversTest {

  private static final Urn SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

  private final EntityClient entityClient = mock(EntityClient.class);
  private final GraphClient graphClient = mock(GraphClient.class);
  private final TimeseriesAspectService timeseriesAspectService =
      mock(TimeseriesAspectService.class);
  private final SiblingGraphService siblingGraphService = mock(SiblingGraphService.class);
  private final BrowsableEntityType<?, ?> browsableType = mock(BrowsableEntityType.class);
  private final DocumentService documentService = mock(DocumentService.class);
  private final ViewService viewService = mock(ViewService.class);

  @DataProvider(name = "guardedResolvers")
  public Object[][] guardedResolvers() {
    return new Object[][] {
      {
        new EntityRelationshipsResultResolver(graphClient),
        Map.of("types", List.of("DownstreamOf"), "direction", "INCOMING"),
        (Consumer<Object>)
            result -> {
              assertEquals(((EntityRelationshipsResult) result).getTotal(), 0);
              assertTrue(((EntityRelationshipsResult) result).getRelationships().isEmpty());
            }
      },
      {
        new EntityBrowsePathsResolver(browsableType),
        Map.of(),
        (Consumer<Object>) result -> assertTrue(((List<?>) result).isEmpty())
      },
      {
        new EntityLineageResultResolver(
            siblingGraphService,
            mock(RestrictedService.class),
            mock(AuthorizationConfiguration.class),
            null),
        Map.of("direction", "UPSTREAM"),
        (Consumer<Object>)
            result -> {
              assertEquals(((EntityLineageResult) result).getTotal(), 0);
              assertTrue(((EntityLineageResult) result).getRelationships().isEmpty());
            }
      },
      {
        new EntityHealthResolver(entityClient, graphClient, timeseriesAspectService),
        Map.of(),
        (Consumer<Object>)
            result -> {
              final List<?> healths = (List<?>) result;
              assertEquals(healths.size(), 1);
              assertEquals(((Health) healths.get(0)).getType(), HealthStatusType.INCIDENTS);
              assertEquals(((Health) healths.get(0)).getStatus(), HealthStatus.PASS);
            }
      },
      {
        new EntityAssertionsResolver(entityClient, graphClient),
        Map.of(),
        (Consumer<Object>)
            result -> {
              assertEquals(((EntityAssertionsResult) result).getTotal(), 0);
              assertTrue(((EntityAssertionsResult) result).getAssertions().isEmpty());
            }
      },
      {
        new EntityRunsResolver(entityClient),
        Map.of(),
        (Consumer<Object>)
            result -> {
              assertEquals(((DataProcessInstanceResult) result).getTotal(), 0);
              assertTrue(((DataProcessInstanceResult) result).getRuns().isEmpty());
            }
      },
      {
        new EntityIncidentsResolver(entityClient),
        Map.of(),
        (Consumer<Object>)
            result -> {
              assertEquals(((EntityIncidentsResult) result).getTotal(), 0);
              assertTrue(((EntityIncidentsResult) result).getIncidents().isEmpty());
            }
      },
      {
        new EntityDataContractResolver(entityClient, graphClient),
        Map.of(),
        (Consumer<Object>) result -> assertNull(result)
      },
      {
        new ParentContainersResolver(entityClient, null),
        Map.of(),
        (Consumer<Object>)
            result -> {
              assertEquals(((ParentContainersResult) result).getCount(), 0);
              assertTrue(((ParentContainersResult) result).getContainers().isEmpty());
            }
      },
      {
        new SiblingsSearchResolver(entityClient, viewService, null),
        Map.of("query", "*"),
        (Consumer<Object>)
            result -> {
              assertEquals(((ScrollResults) result).getTotal(), 0);
              assertTrue(((ScrollResults) result).getSearchResults().isEmpty());
            }
      },
      {
        new RelatedDocumentsResolver(documentService, entityClient),
        Map.of(),
        (Consumer<Object>)
            result -> {
              assertEquals(((RelatedDocumentsResult) result).getTotal(), 0);
              assertTrue(((RelatedDocumentsResult) result).getDocuments().isEmpty());
            }
      },
    };
  }

  @Test(dataProvider = "guardedResolvers")
  public void testUnviewableSourceAnswersLikeMissingEntity(
      final DataFetcher<CompletableFuture<?>> resolver,
      final Map<String, Object> input,
      final Consumer<Object> assertMissingResult)
      throws Exception {
    final Dataset source = new Dataset();
    source.setUrn(SOURCE_URN.toString());
    source.setType(EntityType.DATASET);

    final QueryContext context = denyingViewContext();
    final DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getContext()).thenReturn(context);
    when(environment.getSource()).thenReturn(source);
    when(environment.getArgument(eq("input"))).thenReturn(input);
    when(environment.getArgumentOrDefault(anyString(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    assertMissingResult.accept(resolver.get(environment).join());

    verifyNoInteractions(
        entityClient,
        graphClient,
        timeseriesAspectService,
        siblingGraphService,
        browsableType,
        documentService,
        viewService);
  }

  private static QueryContext denyingViewContext() {
    final Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));
    final Authentication authentication = new Authentication(new Actor(ActorType.USER, "test"), "");
    final OperationContext opContext =
        TestOperationContexts.systemContext(
                () ->
                    OperationContextConfig.builder()
                        .viewAuthorizationConfiguration(
                            ViewAuthorizationConfiguration.builder().enabled(true).build())
                        .build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null)
            .asSession(RequestContext.TEST, authorizer, authentication);

    final QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    return context;
  }
}
