package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
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
  final String documentUrn = "urn:li:document:test-document";
  final String domainUrn = "urn:li:domain:test-domain";
  final String dataProductUrn = "urn:li:dataProduct:test-product";
  final String productDomainUrn = "urn:li:domain:product-domain";

  private DataFetchingEnvironment setUpTestWithPermissions(Entity entity) {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    return mockEnv;
  }

  private DataFetchingEnvironment setUpTestWithResourcePrivilege(Entity entity, String privilege) {
    QueryContext mockContext =
        getMockAllowContextForResource(
            "urn:li:corpuser:test", privilege, UrnUtils.getUrn(entity.getUrn()));
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
    assertTrue(result.getCanEditLifecycleStage());
  }

  @Test
  public void testGetTermCanEditLifecycleStageWithOnlyLifecyclePrivilege() throws Exception {
    final GlossaryTerm glossaryTerm = new GlossaryTerm();
    glossaryTerm.setUrn(glossaryTermUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        setUpTestWithContext(glossaryTerm, contextGrantingOnly("EDIT_ENTITY_LIFECYCLE_STAGE"));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanEditLifecycleStage());
    assertFalse(result.getCanManageEntity());
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
    assertFalse(result.getCanEditLifecycleStage());
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
    assertTrue(result.getCanDeleteEntity());
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
    assertFalse(result.getCanDeleteEntity());
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
  public void testGetDocumentPrivileges() throws Exception {
    final Document document = new Document();
    document.setUrn(documentUrn);
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    EntityPrivileges result =
        new EntityPrivilegesResolver(mockClient).get(setUpTestWithPermissions(document)).get();
    assertTrue(result.getCanManageEntity());
    assertTrue(result.getCanDeleteEntity());

    result =
        new EntityPrivilegesResolver(mockClient).get(setUpTestWithoutPermissions(document)).get();
    assertFalse(result.getCanManageEntity());
    assertFalse(result.getCanDeleteEntity());
  }

  @Test
  public void testGetDocumentPrivilegesSeparatesDeleteFromMove() throws Exception {
    final Document document = new Document();
    document.setUrn(documentUrn);
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Document owners get DELETE_ENTITY only, via the default document-owner delete policy.
    EntityPrivileges result =
        new EntityPrivilegesResolver(mockClient)
            .get(setUpTestWithResourcePrivilege(document, "DELETE_ENTITY"))
            .get();
    assertTrue(result.getCanDeleteEntity());
    assertFalse(result.getCanManageEntity());

    // Editors get EDIT_ENTITY only, so they can move but not delete.
    result =
        new EntityPrivilegesResolver(mockClient)
            .get(setUpTestWithResourcePrivilege(document, "EDIT_ENTITY"))
            .get();
    assertTrue(result.getCanManageEntity());
    assertFalse(result.getCanDeleteEntity());
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
   * {@code canViewQueries} must be true for an actor holding ONLY the platform-level
   * VIEW_ALL_QUERIES privilege — VIEW_ENTITY_QUERIES itself is explicitly denied here — so the
   * Queries/View Definition tab gate is consistent with what {@code
   * EntityAspectAuthorizationUtils#filterViewableQueryEntities} would actually grant once the tab
   * loads.
   */
  @Test
  public void testCanViewQueriesGrantedViaViewAllQueriesAlone() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        setUpTestWithContext(
            dataset,
            contextGrantingOnly(
                com.linkedin.metadata.authorization.PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE
                    .getType()));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanViewQueries());
  }

  /** Baseline: the ordinary dataset-scoped privilege alone still grants canViewQueries. */
  @Test
  public void testCanViewQueriesGrantedViaViewEntityQueriesAlone() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        setUpTestWithContext(
            dataset,
            contextGrantingOnly(
                com.linkedin.metadata.authorization.PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE
                    .getType()));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanViewQueries());
  }

  /** Neither privilege held: canViewQueries must be false. */
  @Test
  public void testCanViewQueriesDeniedWithoutEitherPrivilege() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithContext(dataset, contextGrantingOnly());

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanViewQueries());
  }

  /**
   * The escape valve: with query-view authorization disabled entirely ({@code
   * QUERY_ENTITY_AUTHORIZATION_ENABLED=false} and the general view-authorization switch also off),
   * canViewQueries reads as granted even for an actor holding neither privilege — matching
   * ListQueriesResolver/QueryType's identical guard, so the UI doesn't show a denial message when
   * the backend isn't actually enforcing anything.
   */
  @Test
  public void testCanViewQueriesGrantedWhenQueryAuthorizationDisabled() throws Exception {
    final Dataset dataset = new Dataset();
    dataset.setUrn(datasetUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        setUpTestWithContext(
            dataset,
            contextWithViewAuthConfig(
                com.datahub.authorization.config.ViewAuthorizationConfiguration.builder()
                    .enabled(false)
                    .queryEntities(
                        com.datahub.authorization.config.ViewAuthorizationConfiguration
                            .QueryEntityAuthorizationConfig.builder()
                            .enabled(false)
                            .build())
                    .build()));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanViewQueries());
  }

  @Test
  public void testGetDomainCanManageDataProductsWithManageDataProductsPrivilege() throws Exception {
    Domain domain = new Domain();
    domain.setUrn(domainUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        setUpTestWithContext(domain, contextGrantingOnly("MANAGE_DATA_PRODUCTS"));

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertTrue(result.getCanManageDataProducts());
  }

  @Test
  public void testGetDomainCannotManageDataProductsWithoutPrivilege() throws Exception {
    Domain domain = new Domain();
    domain.setUrn(domainUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithContext(domain, contextGrantingOnly());

    EntityPrivilegesResolver resolver = new EntityPrivilegesResolver(mockClient);
    EntityPrivileges result = resolver.get(mockEnv).get();

    assertFalse(result.getCanManageDataProducts());
  }

  @Test
  public void testGetDataProductPrivilegesWithoutDomainFallsBackToEditEntity() throws Exception {
    EntityPrivileges result = getDataProductPrivileges(contextGrantingOnly("EDIT_ENTITY"));

    assertTrue(result.getCanManageEntity());
    assertTrue(result.getCanDeleteEntity());
  }

  @Test
  public void testGetDataProductPrivilegesWithOnlyDeletePrivilege() throws Exception {
    EntityPrivileges result = getDataProductPrivileges(contextGrantingOnly("DELETE_ENTITY"));

    assertFalse(result.getCanManageEntity());
    assertTrue(result.getCanDeleteEntity());
  }

  @Test
  public void testGetDataProductPrivilegesUnauthorized() throws Exception {
    EntityPrivileges result = getDataProductPrivileges(contextGrantingOnly());

    assertFalse(result.getCanManageEntity());
    assertFalse(result.getCanDeleteEntity());
  }

  @Test
  public void testGetDataProductPrivilegesFollowDomainManageDataProducts() throws Exception {
    // EDIT_ENTITY on the product only applies to products without a live domain.
    EntityPrivileges editOnly =
        getDataProductPrivilegesInDomain(
            getMockAllowContextForResource(
                "urn:li:corpuser:test", "EDIT_ENTITY", UrnUtils.getUrn(dataProductUrn)));
    assertFalse(editOnly.getCanManageEntity());
    assertFalse(editOnly.getCanDeleteEntity());

    EntityPrivileges domainManager =
        getDataProductPrivilegesInDomain(
            getMockAllowContextForResource(
                "urn:li:corpuser:test", "MANAGE_DATA_PRODUCTS", UrnUtils.getUrn(productDomainUrn)));
    assertTrue(domainManager.getCanManageEntity());
    assertTrue(domainManager.getCanDeleteEntity());
  }

  @Test
  public void testGetDataProductPrivilegesDomainReadFailureDeniesManage() throws Exception {
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getLatestAspectObject(any(), any(), any()))
        .thenThrow(new RuntimeException("gms down"));

    EntityPrivileges result =
        getDataProductPrivileges(
            withAspectRetriever(contextGrantingOnly("EDIT_ENTITY"), aspectRetriever));

    assertFalse(result.getCanManageEntity());
  }

  private EntityPrivileges getDataProductPrivilegesInDomain(QueryContext context) throws Exception {
    Urn productUrn = UrnUtils.getUrn(dataProductUrn);
    Urn domain = UrnUtils.getUrn(productDomainUrn);
    Domains domains = new Domains().setDomains(new UrnArray(List.of(domain)));

    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(
            aspectRetriever.getLatestAspectObject(
                any(), Mockito.eq(productUrn), Mockito.eq(Constants.DOMAINS_ASPECT_NAME)))
        .thenReturn(new Aspect(domains.data()));
    Mockito.when(aspectRetriever.entityExists(any(), any())).thenReturn(Map.of(domain, true));

    return getDataProductPrivileges(withAspectRetriever(context, aspectRetriever));
  }

  private QueryContext withAspectRetriever(QueryContext context, AspectRetriever aspectRetriever) {
    OperationContext opContext = Mockito.spy(context.getOperationContext());
    Mockito.when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    Mockito.when(context.getOperationContext()).thenReturn(opContext);
    return context;
  }

  private EntityPrivileges getDataProductPrivileges(QueryContext context) throws Exception {
    DataProduct dataProduct = new DataProduct();
    dataProduct.setUrn(dataProductUrn);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = setUpTestWithContext(dataProduct, context);

    return new EntityPrivilegesResolver(mockClient).get(mockEnv).get();
  }

  private QueryContext contextWithViewAuthConfig(
      com.datahub.authorization.config.ViewAuthorizationConfiguration
          viewAuthorizationConfiguration) {
    com.datahub.plugins.auth.authorization.Authorizer mockAuthorizer =
        Mockito.mock(com.datahub.plugins.auth.authorization.Authorizer.class);
    Mockito.when(
            mockAuthorizer.authorize(
                Mockito.any(com.datahub.authorization.AuthorizationRequest.class)))
        .thenReturn(
            new com.datahub.authorization.AuthorizationResult(
                null, com.datahub.authorization.AuthorizationResult.Type.DENY, ""));

    Authentication authentication =
        new Authentication(
            new com.datahub.authentication.Actor(com.datahub.authentication.ActorType.USER, "test"),
            "creds");

    io.datahubproject.metadata.context.OperationContext systemContext =
        io.datahubproject.test.metadata.context.TestOperationContexts.systemContext(
            () ->
                io.datahubproject.metadata.context.OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(viewAuthorizationConfiguration)
                    .build(),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    io.datahubproject.metadata.context.OperationContext opContext =
        systemContext.asSession(
            io.datahubproject.metadata.context.RequestContext.TEST, mockAuthorizer, authentication);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(authentication);
    Mockito.when(mockContext.getOperationContext()).thenReturn(opContext);
    return mockContext;
  }

  private DataFetchingEnvironment setUpTestWithContext(Entity entity, QueryContext context) {
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(context);
    Mockito.when(mockEnv.getSource()).thenReturn(entity);
    return mockEnv;
  }

  /**
   * A context whose authorizer grants exactly the given privilege strings (matched by name alone,
   * regardless of resource) and denies everything else.
   */
  private QueryContext contextGrantingOnly(String... allowedPrivileges) {
    java.util.Set<String> allowed = java.util.Set.of(allowedPrivileges);
    com.datahub.plugins.auth.authorization.Authorizer mockAuthorizer =
        Mockito.mock(com.datahub.plugins.auth.authorization.Authorizer.class);
    Mockito.when(
            mockAuthorizer.authorize(
                Mockito.any(com.datahub.authorization.AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              com.datahub.authorization.AuthorizationRequest request = invocation.getArgument(0);
              boolean isAllowed = allowed.contains(request.getPrivilege());
              return new com.datahub.authorization.AuthorizationResult(
                  request,
                  isAllowed
                      ? com.datahub.authorization.AuthorizationResult.Type.ALLOW
                      : com.datahub.authorization.AuthorizationResult.Type.DENY,
                  "");
            });

    Authentication authentication =
        new Authentication(
            new com.datahub.authentication.Actor(com.datahub.authentication.ActorType.USER, "test"),
            "creds");

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(authentication);
    io.datahubproject.metadata.context.OperationContext operationContext =
        io.datahubproject.test.metadata.context.TestOperationContexts
            .userContextNoSearchAuthorization(mockAuthorizer, authentication);
    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext);
    return mockContext;
  }
}
