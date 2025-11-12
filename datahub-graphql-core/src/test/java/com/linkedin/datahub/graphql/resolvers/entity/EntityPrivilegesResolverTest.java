package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityPrivilegesResolverTest {

  final String glossaryTermUrn = "urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451";
  final String glossaryNodeUrn = "urn:li:glossaryNode:11115397daf94708a8822b8106cfd451";
  final String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageA,TEST)";
  final String chartUrn = "urn:li:chart:(looker,baz1)";
  final String dashboardUrn = "urn:li:dashboard:(looker,dashboards.1)";
  final String dataJobUrn =
      "urn:li:dataJob:(urn:li:dataFlow:(spark,test_machine.sparkTestApp,local),QueryExecId_31)";
  final String documentUrn = "urn:li:document:test-document";

  private DataFetchingEnvironment setUpTestWithPermissions(Entity entity) {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    return mockEnv;
  }

  @Test
  public void testGetTermSuccessWithPermissions() throws Exception {
    final GlossaryTerm glossaryTerm = new GlossaryTerm();
    glossaryTerm.setUrn(glossaryTermUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(glossaryTerm);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanManageEntity());
  }

  @Test
  public void testGetNodeSuccessWithPermissions() throws Exception {
    final GlossaryNode glossaryNode = new GlossaryNode();
    glossaryNode.setUrn(glossaryNodeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(glossaryNode);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanManageEntity());
    assertTrue(result.getCanManageChildren());
  }

  private DataFetchingEnvironment setUpTestWithoutPermissions(Entity entity) {
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    return mockEnv;
  }

  @Test
  public void testGetTermSuccessWithoutPermissions() throws Exception {
    final GlossaryTerm glossaryTerm = new GlossaryTerm();
    glossaryTerm.setUrn(glossaryTermUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(glossaryTerm);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanManageEntity());
  }

  @Test
  public void testGetNodeSuccessWithoutPermissions() throws Exception {
    final GlossaryNode glossaryNode = new GlossaryNode();
    glossaryNode.setUrn(glossaryNodeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(glossaryNode);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanManageEntity());
    assertFalse(result.getCanManageChildren());
  }

  @Test
  public void testGetFailure() throws Exception {
    final GlossaryNode glossaryNode = new GlossaryNode();
    glossaryNode.setUrn(glossaryNodeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(glossaryNode);

    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .getV2(
            any(), Mockito.eq(Constants.GLOSSARY_NODE_ENTITY_NAME), Mockito.any(), Mockito.any());

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetDatasetSuccessWithPermissions() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(dataset);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanEditQueries());
    assertTrue(result.getCanEditLineage());
  }

  @Test
  public void testGetDatasetSuccessWithoutPermissions() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(dataset);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanEditQueries());
    assertFalse(result.getCanEditLineage());
  }

  @Test
  public void testGetChartSuccessWithPermissions() throws Exception {
    final Chart chart = new Chart();
    chart.setUrn(chartUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(chart);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanEditLineage());
  }

  @Test
  public void testGetChartSuccessWithoutPermissions() throws Exception {
    final Chart chart = new Chart();
    chart.setUrn(chartUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(chart);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanEditLineage());
  }

  @Test
  public void testGetDashboardSuccessWithPermissions() throws Exception {
    final Dashboard dashboard = new Dashboard();
    dashboard.setUrn(dashboardUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(dashboard);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanEditLineage());
  }

  @Test
  public void testGetDashboardSuccessWithoutPermissions() throws Exception {
    final Dashboard dashboard = new Dashboard();
    dashboard.setUrn(dashboardUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(dashboard);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanEditLineage());
  }

  @Test
  public void testGetDataJobSuccessWithPermissions() throws Exception {
    final DataJob dataJob = new DataJob();
    dataJob.setUrn(dataJobUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(dataJob);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanEditLineage());
  }

  @Test
  public void testGetDataJobSuccessWithoutPermissions() throws Exception {
    final DataJob dataJob = new DataJob();
    dataJob.setUrn(dataJobUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(dataJob);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanEditLineage());
  }

  @Test
  public void testGetDocumentSuccessWithPermissions() throws Exception {
    final Document document = new Document();
    document.setUrn(documentUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(document);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    // Documents fall through to default case, so only common privileges are set
    assertTrue(result.getCanEditLineage());
    assertTrue(result.getCanEditProperties());
    assertTrue(result.getCanEditDomains());
    assertTrue(result.getCanEditDeprecation());
    assertTrue(result.getCanEditGlossaryTerms());
    assertTrue(result.getCanEditTags());
    assertTrue(result.getCanEditOwners());
    assertTrue(result.getCanEditDescription());
    assertTrue(result.getCanEditLinks());
  }

  @Test
  public void testGetDocumentSuccessWithoutPermissions() throws Exception {
    final Document document = new Document();
    document.setUrn(documentUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(document);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    // Documents fall through to default case, so only common privileges are set
    assertFalse(result.getCanEditLineage());
    assertFalse(result.getCanEditProperties());
    assertFalse(result.getCanEditDomains());
    assertFalse(result.getCanEditDeprecation());
    assertFalse(result.getCanEditGlossaryTerms());
    assertFalse(result.getCanEditTags());
    assertFalse(result.getCanEditOwners());
    assertFalse(result.getCanEditDescription());
    assertFalse(result.getCanEditLinks());
  }
}
