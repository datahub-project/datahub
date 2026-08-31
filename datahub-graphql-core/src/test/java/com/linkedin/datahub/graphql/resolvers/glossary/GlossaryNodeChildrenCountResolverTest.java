package com.linkedin.datahub.graphql.resolvers.glossary;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GlossaryNodeChildrenCount;
import com.linkedin.datahub.graphql.resolvers.load.GlossaryNodeChildrenCountBatchLoader;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryNodeChildrenCountResolverTest {
  private static final String TEST_GLOSSARY_NODE_URN = "urn:li:glossaryNode:test-id";

  private DataFetchingEnvironment _dataFetchingEnvironment;
  private GlossaryNodeChildrenCountResolver _resolver;
  private Entity _entity;
  private DataLoaderRegistry _registry;
  private List<List<String>> _observedBatches;

  @BeforeMethod
  public void setupTest() {
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _entity = Mockito.mock(Entity.class);
    Mockito.when(_entity.getUrn()).thenReturn(TEST_GLOSSARY_NODE_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(_entity);

    _observedBatches = new ArrayList<>();
    final DataLoader<String, GlossaryNodeChildrenCount> loader =
        DataLoader.newDataLoader(
            keys -> {
              _observedBatches.add(new ArrayList<>(keys));
              return CompletableFuture.completedFuture(
                  keys.stream()
                      .map(
                          key -> {
                            final GlossaryNodeChildrenCount count = new GlossaryNodeChildrenCount();
                            count.setTermsCount(5);
                            count.setNodesCount(3);
                            return count;
                          })
                      .collect(Collectors.toList()));
            });
    _registry = new DataLoaderRegistry();
    _registry.register(GlossaryNodeChildrenCountBatchLoader.LOADER_NAME, loader);
    Mockito.when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(_registry);

    _resolver = new GlossaryNodeChildrenCountResolver();
  }

  @Test
  public void testGetLoadsTheSourceUrnThroughTheBatchLoader() throws Exception {
    final CompletableFuture<GlossaryNodeChildrenCount> future =
        _resolver.get(_dataFetchingEnvironment);
    _registry.dispatchAll();

    final GlossaryNodeChildrenCount result = future.get();
    assertEquals(result.getTermsCount(), 5);
    assertEquals(result.getNodesCount(), 3);
    assertEquals(_observedBatches, List.of(List.of(TEST_GLOSSARY_NODE_URN)));
  }

  @Test
  public void testGetInvalidUrn() {
    Mockito.when(_entity.getUrn()).thenReturn("invalid-urn");

    assertThrows(URISyntaxException.class, () -> _resolver.get(_dataFetchingEnvironment));
  }
}
