package com.linkedin.datahub.graphql.resolvers.browse;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.ViewAuthorizationTestContexts.contextHiding;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowsePathsInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BrowsePathsResolverTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final List<String> PATH = ImmutableList.of("prod", "mysql");

  private BrowsableEntityType<?, ?> _datasetType;
  private DataFetchingEnvironment _environment;
  private BrowsePathsResolver _resolver;

  @BeforeMethod
  public void setUp() throws Exception {
    _datasetType = mock(BrowsableEntityType.class);
    when(_datasetType.type()).thenReturn(EntityType.DATASET);
    when(_datasetType.browsePaths(eq(DATASET_URN), any()))
        .thenReturn(ImmutableList.of(new BrowsePath(PATH)));
    _resolver = new BrowsePathsResolver(ImmutableList.of(_datasetType));

    _environment = mock(DataFetchingEnvironment.class);
    final QueryContext context = getMockAllowContext();
    when(_environment.getContext()).thenReturn(context);
    final BrowsePathsInput input = new BrowsePathsInput();
    input.setType(EntityType.DATASET);
    input.setUrn(DATASET_URN);
    when(_environment.getArgument(eq("input"))).thenReturn(input);
  }

  @Test
  public void testViewableEntityReturnsItsBrowsePaths() throws Exception {
    final List<BrowsePath> result = _resolver.get(_environment).join();

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPath(), PATH);
  }

  @Test
  public void testUnviewableEntityAnswersAsMissing() throws Exception {
    final QueryContext context = contextHiding(UrnUtils.getUrn(DATASET_URN));
    when(_environment.getContext()).thenReturn(context);

    assertTrue(_resolver.get(_environment).join().isEmpty());
    verify(_datasetType, never()).browsePaths(any(), any());
  }

  @Test
  public void testUnviewableUrnOfAnotherTypeGoesToTheRequestedType() throws Exception {
    final String chartUrn = "urn:li:chart:(looker,hidden)";
    final QueryContext context = contextHiding(UrnUtils.getUrn(chartUrn));
    when(_environment.getContext()).thenReturn(context);
    final BrowsePathsInput input = new BrowsePathsInput();
    input.setType(EntityType.DATASET);
    input.setUrn(chartUrn);
    when(_environment.getArgument(eq("input"))).thenReturn(input);

    _resolver.get(_environment).join();

    verify(_datasetType).browsePaths(eq(chartUrn), any());
  }
}
