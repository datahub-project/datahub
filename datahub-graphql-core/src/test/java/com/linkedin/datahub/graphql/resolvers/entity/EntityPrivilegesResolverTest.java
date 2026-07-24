package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.MergedField;
import graphql.language.Field;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Set;
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
  final String businessAttributeUrn = "urn:li:businessAttribute:testBusinessAttribute";

  // Every field on the EntityPrivileges GraphQL type (auth.graphql). Used as the default selection
  // so existing assertions (which expect all applicable fields populated) keep passing.
  private static final Set<String> ALL_PRIVILEGE_FIELDS =
      Set.of(
          "canManageChildren",
          "canManageEntity",
          "canEditLineage",
          "canEditEmbed",
          "canEditQueries",
          "canEditProperties",
          "canEditTags",
          "canEditGlossaryTerms",
          "canEditDescription",
          "canEditLinks",
          "canEditDomains",
          "canEditDataProducts",
          "canEditOwners",
          "canEditIncidents",
          "canEditAssertions",
          "canEditAssertionOwners",
          "canEditDeprecation",
          "canEditSchemaFieldTags",
          "canEditSchemaFieldGlossaryTerms",
          "canEditSchemaFieldDescription",
          "canViewDatasetUsage",
          "canViewDatasetProfile",
          "canViewDatasetOperations",
          "canManageAssetSummary");

  // Builds a real query AST for `privileges { <fieldNames> }` so the resolver reads the selected
  // sub-fields the same way it does at runtime (from the field's selection set), exercising the
  // actual code path rather than a mocked selection set.
  private static MergedField mockMergedField(Set<String> fieldNames) {
    SelectionSet.Builder selectionSet = SelectionSet.newSelectionSet();
    for (String name : fieldNames) {
      selectionSet.selection(Field.newField(name).build());
    }
    Field privileges = Field.newField("privileges").selectionSet(selectionSet.build()).build();
    return MergedField.newMergedField(privileges).build();
  }

  private DataFetchingEnvironment setUpTestWithPermissions(Entity entity) {
    return setUpTestWithPermissions(entity, ALL_PRIVILEGE_FIELDS);
  }

  private DataFetchingEnvironment setUpTestWithPermissions(
      Entity entity, Set<String> selectedFields) {
    MergedField mergedField = mockMergedField(selectedFields);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    Mockito.when(mockEnv.getMergedField()).thenReturn(mergedField);
    Mockito.when(mockEnv.getFragmentsByName()).thenReturn(Collections.emptyMap());
    return mockEnv;
  }

  private DataFetchingEnvironment setUpTestWithoutPermissions(Entity entity) {
    return setUpTestWithoutPermissions(entity, ALL_PRIVILEGE_FIELDS);
  }

  private DataFetchingEnvironment setUpTestWithoutPermissions(
      Entity entity, Set<String> selectedFields) {
    MergedField mergedField = mockMergedField(selectedFields);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    Mockito.when(mockEnv.getMergedField()).thenReturn(mergedField);
    Mockito.when(mockEnv.getFragmentsByName()).thenReturn(Collections.emptyMap());
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
  public void testGetBusinessAttributeSuccessWithPermissions() throws Exception {
    final BusinessAttribute businessAttribute = new BusinessAttribute();
    businessAttribute.setUrn(businessAttributeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(businessAttribute);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanManageEntity());
    // Verify common privileges are also set
    assertTrue(result.getCanEditLineage());
    assertTrue(result.getCanEditProperties());
    assertTrue(result.getCanEditTags());
    assertTrue(result.getCanEditGlossaryTerms());
    assertTrue(result.getCanEditOwners());
    assertTrue(result.getCanEditAssertionOwners());
    assertTrue(result.getCanEditDescription());
    assertTrue(result.getCanEditLinks());
  }

  @Test
  public void testGetBusinessAttributeSuccessWithoutPermissions() throws Exception {
    final BusinessAttribute businessAttribute = new BusinessAttribute();
    businessAttribute.setUrn(businessAttributeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithoutPermissions(businessAttribute);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanManageEntity());
    // Verify common privileges are also denied
    assertFalse(result.getCanEditLineage());
    assertFalse(result.getCanEditProperties());
    assertFalse(result.getCanEditTags());
    assertFalse(result.getCanEditGlossaryTerms());
    assertFalse(result.getCanEditOwners());
    assertFalse(result.getCanEditAssertionOwners());
    assertFalse(result.getCanEditDescription());
    assertFalse(result.getCanEditLinks());
  }

  @Test
  public void testGetBusinessAttributeWithCommonPrivileges() throws Exception {
    final BusinessAttribute businessAttribute = new BusinessAttribute();
    businessAttribute.setUrn(businessAttributeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(businessAttribute);

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    // Test that business attribute gets all common privileges
    assertNotNull(result);
    assertTrue(result.getCanManageEntity());
    assertTrue(result.getCanEditLineage());
    assertTrue(result.getCanEditProperties());
    assertTrue(result.getCanEditAssertions());
    assertTrue(result.getCanEditAssertionOwners());
    assertTrue(result.getCanEditIncidents());
    assertTrue(result.getCanEditDomains());
    assertTrue(result.getCanEditDataProducts());
    assertTrue(result.getCanEditDeprecation());
    assertTrue(result.getCanEditGlossaryTerms());
    assertTrue(result.getCanEditTags());
    assertTrue(result.getCanEditOwners());
    assertTrue(result.getCanEditDescription());
    assertTrue(result.getCanEditLinks());
    assertTrue(result.getCanManageAssetSummary());
  }

  /**
   * Selection-set awareness: when the query selects only {@code canEditLineage} (as the lineage
   * canEditLineageFragment does), only that privilege is computed. Every other privilege is left
   * unset (null) rather than computed-and-discarded — this is the optimization that avoids the
   * per-entity authorization fan-out on the lineage path.
   */
  @Test
  public void testComputesOnlySelectedPrivileges() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(dataset, Set.of("canEditLineage"));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    // The single selected field is computed.
    assertTrue(result.getCanEditLineage());
    // Unselected fields are NOT computed (remain null).
    assertNull(result.getCanEditQueries());
    assertNull(result.getCanEditTags());
    assertNull(result.getCanEditDomains());
    assertNull(result.getCanEditOwners());
    assertNull(result.getCanViewDatasetProfile());
    assertNull(result.getCanManageAssetSummary());
  }

  /**
   * Guard against a typo in any selection-set field-name string. With an allow context, every
   * EntityPrivileges field that a Dataset supports must be computed (non-null) when selected; a
   * mismatched field name would silently leave the field null and fail this test. Dataset covers
   * all fields except canManageEntity / canManageChildren (asserted via glossary tests).
   */
  @Test
  public void testAllDatasetFieldsHandledWithCorrectNames() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithPermissions(dataset); // selects ALL fields

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    // Dataset-specific fields
    assertNotNull(result.getCanEditEmbed());
    assertNotNull(result.getCanEditQueries());
    assertNotNull(result.getCanEditSchemaFieldTags());
    assertNotNull(result.getCanEditSchemaFieldGlossaryTerms());
    assertNotNull(result.getCanEditSchemaFieldDescription());
    assertNotNull(result.getCanViewDatasetUsage());
    assertNotNull(result.getCanViewDatasetProfile());
    assertNotNull(result.getCanViewDatasetOperations());
    // Common fields
    assertNotNull(result.getCanEditLineage());
    assertNotNull(result.getCanEditProperties());
    assertNotNull(result.getCanEditAssertions());
    assertNotNull(result.getCanEditAssertionOwners());
    assertNotNull(result.getCanEditIncidents());
    assertNotNull(result.getCanEditDomains());
    assertNotNull(result.getCanEditDataProducts());
    assertNotNull(result.getCanEditDeprecation());
    assertNotNull(result.getCanEditGlossaryTerms());
    assertNotNull(result.getCanEditTags());
    assertNotNull(result.getCanEditOwners());
    assertNotNull(result.getCanEditDescription());
    assertNotNull(result.getCanEditLinks());
    assertNotNull(result.getCanManageAssetSummary());
  }

  /**
   * Selection-set awareness for the glossary-node manage fields: selecting only canManageChildren
   * must not compute canManageEntity (and the parent-chain walk it would trigger).
   */
  @Test
  public void testGlossaryNodeComputesOnlySelectedManageField() throws Exception {
    final GlossaryNode glossaryNode = new GlossaryNode();
    glossaryNode.setUrn(glossaryNodeUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        setUpTestWithPermissions(glossaryNode, Set.of("canManageChildren"));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanManageChildren());
    assertNull(result.getCanManageEntity());
  }
}
