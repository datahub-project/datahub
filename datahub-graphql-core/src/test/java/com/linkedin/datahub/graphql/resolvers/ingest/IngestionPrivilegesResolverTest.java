package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourcePrivileges;
import com.linkedin.datahub.graphql.resolvers.ingest.privileges.IngestionSourcePrivilegesResolver;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestionPrivilegesResolverTest {

  final String ingestionSourceUrn = "urn:li:dataHubIngestionSource:test";

  private DataFetchingEnvironment setUpTestWithPermissions(IngestionSource entity) {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    return mockEnv;
  }

  private DataFetchingEnvironment setUpTestWithoutPermissions(IngestionSource entity) {
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    return mockEnv;
  }

  @Test
  public void testGetIngestionSourceWithPermissions() throws Exception {
    final IngestionSource source = new IngestionSource();
    source.setUrn(ingestionSourceUrn);

    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(source);

    IngestionSourcePrivilegesResolver resolver = new IngestionSourcePrivilegesResolver();
    IngestionSourcePrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanEdit());
    assertTrue(result.getCanDelete());
    assertTrue(result.getCanExecute());
    assertTrue(result.getCanView());
  }

  @Test
  public void testGetIngestionSourceWithoutPermissions() throws Exception {
    final IngestionSource source = new IngestionSource();
    source.setUrn(ingestionSourceUrn);

    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(source);

    IngestionSourcePrivilegesResolver resolver = new IngestionSourcePrivilegesResolver();
    IngestionSourcePrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanEdit());
    assertFalse(result.getCanDelete());
    assertFalse(result.getCanExecute());
    assertFalse(result.getCanView());
  }
}
