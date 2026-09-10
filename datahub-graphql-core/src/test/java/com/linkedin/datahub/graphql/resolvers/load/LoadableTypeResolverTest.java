package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.types.domain.DomainType;
import com.linkedin.datahub.graphql.types.restricted.RestrictedType;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.Test;

public class LoadableTypeResolverTest {

  private static final String DOMAIN_URN = "urn:li:domain:internal-domain-0003";

  @Test
  public void testReturnsDomainWhenAuthorized() throws Exception {
    final Domain domain = new Domain();
    domain.setUrn(DOMAIN_URN);
    domain.setType(EntityType.DOMAIN);

    final DomainType domainType = new DomainType(mock(EntityClient.class));
    final DataFetchingEnvironment env = environmentWithLoader(domainType.name(), domain);
    final LoadableTypeResolver<Domain, String> resolver =
        new LoadableTypeResolver<>(domainType, (e) -> DOMAIN_URN);

    final Domain result = resolver.get(env).get();
    assertSame(result, domain);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRestrictedPlaceholderThrowsAuthorizationException() throws Exception {
    final Restricted restricted = new Restricted();
    restricted.setUrn(DOMAIN_URN);
    restricted.setType(EntityType.RESTRICTED);

    // batchLoad stores Restricted inside DataFetcherResult<Domain> via generic erasure
    final DataFetcherResult<Domain> fetcherResult =
        (DataFetcherResult<Domain>)
            (DataFetcherResult<?>) DataFetcherResult.newResult().data(restricted).build();

    final DomainType domainType = new DomainType(mock(EntityClient.class));
    final DataFetchingEnvironment env = environmentWithLoader(domainType.name(), fetcherResult);
    final LoadableTypeResolver<Domain, String> resolver =
        new LoadableTypeResolver<>(domainType, (e) -> DOMAIN_URN);

    try {
      resolver.get(env).get();
      throw new AssertionError("Expected AuthorizationException");
    } catch (java.util.concurrent.ExecutionException e) {
      assertEquals(e.getCause().getClass(), AuthorizationException.class);
      assertEquals(e.getCause().getMessage(), "Unauthorized to view entity: " + DOMAIN_URN);
    }
  }

  @Test
  public void testRestrictedTypeLoaderAcceptsRestrictedEntity() throws Exception {
    final Restricted restricted = new Restricted();
    restricted.setUrn(DOMAIN_URN);
    restricted.setType(EntityType.RESTRICTED);

    final RestrictedType restrictedType =
        new RestrictedType(mock(EntityClient.class), mock(RestrictedService.class));
    final DataFetchingEnvironment env = environmentWithLoader(restrictedType.name(), restricted);
    final LoadableTypeResolver<Restricted, String> resolver =
        new LoadableTypeResolver<>(restrictedType, (e) -> DOMAIN_URN);

    final Restricted result = resolver.get(env).get();
    assertEquals(result.getUrn(), DOMAIN_URN);
  }

  @SuppressWarnings("unchecked")
  private static DataFetchingEnvironment environmentWithLoader(
      final String loaderName, final Object loaderResult) {
    final DataLoader<String, Object> loader = mock(DataLoader.class);
    when(loader.load(anyString())).thenReturn(CompletableFuture.completedFuture(loaderResult));

    final DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    when(registry.getDataLoader(loaderName)).thenReturn((DataLoader) loader);

    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getDataLoaderRegistry()).thenReturn(registry);
    return env;
  }
}
