package com.linkedin.datahub.graphql.resolvers.browse;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EntityBrowsePathsResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    BrowsableEntityType mockType = Mockito.mock(BrowsableEntityType.class);

    List<String> path = ImmutableList.of("prod", "mysql");
    Mockito.when(mockType.browsePaths(Mockito.eq(TEST_ENTITY_URN), Mockito.any()))
      .thenReturn(ImmutableList.of(
          new BrowsePath(path))
      );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn datasetUrn = Urn.createFromString(TEST_ENTITY_URN);
    Dataset datasetEntity = new Dataset();
    datasetEntity.setUrn(datasetUrn.toString());
    datasetEntity.setType(EntityType.DATASET);
    Mockito.when(mockEnv.getSource()).thenReturn(datasetEntity);

    EntityBrowsePathsResolver resolver = new EntityBrowsePathsResolver(mockType);
    List<BrowsePath> result = resolver.get(mockEnv).get();

    Assert.assertEquals(result.get(0).getPath(), path);
  }

  @Test
  public void testGetBrowsePathsException() throws Exception {
    BrowsableEntityType mockType = Mockito.mock(BrowsableEntityType.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockType).browsePaths(
        Mockito.any(),
        Mockito.any());

    EntityBrowsePathsResolver resolver = new EntityBrowsePathsResolver(mockType);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn datasetUrn = Urn.createFromString(TEST_ENTITY_URN);
    Dataset datasetEntity = new Dataset();
    datasetEntity.setUrn(datasetUrn.toString());
    datasetEntity.setType(EntityType.DATASET);
    Mockito.when(mockEnv.getSource()).thenReturn(datasetEntity);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}