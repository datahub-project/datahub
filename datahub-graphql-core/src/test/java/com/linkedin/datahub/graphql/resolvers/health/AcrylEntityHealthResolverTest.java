package com.linkedin.datahub.graphql.resolvers.health;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActiveIncidentHealthDetails;
import com.linkedin.datahub.graphql.generated.AssertionHealthStatusByType;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AcrylEntityHealthResolverTest {

  private static final String TEST_DATASET_URN = "urn:li:dataset:(test,test,test)";
  private static final String TEST_ASSERTION_URN = "urn:li:assertion:test-guid";
  private static final String TEST_ASSERTION_URN_2 = "urn:li:assertion:test-guid-2";
  private static final String TEST_INCIDENT_URN = "urn:li:incident:test-guid";

  @Test
  public void testGetSuccessHealthyPassingOnly() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType(AssertionType.FRESHNESS.name())
                    .setSource("test")
                    .setLastResultAt(0L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.PASS);

    // Verify latestAssertionStatusByType
    List<AssertionHealthStatusByType> assertionStatusByType =
        result.get(0).getLatestAssertionStatusByType();
    assertNotNull(assertionStatusByType);
    assertEquals(assertionStatusByType.size(), 1);
    assertEquals(assertionStatusByType.get(0).getType().name(), AssertionType.FRESHNESS.name());
    assertEquals(assertionStatusByType.get(0).getStatus(), HealthStatus.PASS);
    assertEquals(assertionStatusByType.get(0).getTotal(), 1);
    assertEquals(assertionStatusByType.get(0).getStatusCount(), 1);
  }

  @Test
  public void testGetSuccessWithActiveIncidents() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    // Mock incident search result
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    searchResult.setEntities(
        new SearchEntityArray(
            ImmutableList.of(new SearchEntity().setEntity(UrnUtils.getUrn(TEST_INCIDENT_URN)))));

    Mockito.when(
            entityClient.filter(
                any(OperationContext.class),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                any(),
                any(),
                Mockito.eq(0),
                Mockito.eq(1)))
        .thenReturn(searchResult);

    // Mock incident info
    IncidentInfo incidentInfo = new IncidentInfo();
    incidentInfo.setTitle("Test Incident");
    IncidentStatus status = new IncidentStatus();
    status.setLastUpdated(new AuditStamp().setTime(0L));
    incidentInfo.setStatus(status);

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_INCIDENT_URN)),
                Mockito.eq(ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME)),
                Mockito.anyBoolean()))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_INCIDENT_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.INCIDENT_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(incidentInfo.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(false, true));

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);

    // Verify activeIncidentHealthDetails
    ActiveIncidentHealthDetails incidentDetails = result.get(0).getActiveIncidentHealthDetails();
    assertNotNull(incidentDetails);
    assertEquals(incidentDetails.getLatestIncidentUrn(), TEST_INCIDENT_URN);
    assertEquals(incidentDetails.getLatestIncidentTitle(), "Test Incident");
    assertEquals(incidentDetails.getLastActivityAt(), 0L);
    assertEquals(incidentDetails.getCount(), 1);
  }

  @Test
  public void testGetSuccessUnhealthyFailingAndPassing() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType(AssertionType.FRESHNESS.name())
                    .setSource("test")
                    .setLastResultAt(0L))));
    summary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN_2))
                    .setType(AssertionType.VOLUME.name())
                    .setSource("test")
                    .setLastResultAt(0L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);

    // Verify latestAssertionStatusByType
    List<AssertionHealthStatusByType> assertionStatusByType =
        result.get(0).getLatestAssertionStatusByType();
    assertNotNull(assertionStatusByType);
    assertEquals(assertionStatusByType.size(), 2);
    assertEquals(assertionStatusByType.get(0).getType().name(), AssertionType.FRESHNESS.name());
    assertEquals(assertionStatusByType.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(assertionStatusByType.get(0).getTotal(), 1);
    assertEquals(assertionStatusByType.get(0).getStatusCount(), 1);
  }

  @Test
  public void testGetSuccessHealthyPassingEmptyFailing() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType("type")
                    .setSource("test")
                    .setLastResultAt(0L))));
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray());

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.PASS);
  }

  @Test
  public void testGetSuccessNullHealthMissingAspect() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(null);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetSuccessInitializingOnly() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setInitializingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType(AssertionType.FRESHNESS.name())
                    .setSource("test")
                    .setLastResultAt(0L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.INIT);
    assertEquals(result.get(0).getMessage(), "1 of 1 assertions are initializing");

    // Verify latestAssertionStatusByType
    List<AssertionHealthStatusByType> assertionStatusByType =
        result.get(0).getLatestAssertionStatusByType();
    assertNotNull(assertionStatusByType);
    assertEquals(assertionStatusByType.size(), 1);
    assertEquals(assertionStatusByType.get(0).getType().name(), AssertionType.FRESHNESS.name());
    assertEquals(assertionStatusByType.get(0).getStatus(), HealthStatus.INIT);
    assertEquals(assertionStatusByType.get(0).getTotal(), 1);
    assertEquals(assertionStatusByType.get(0).getStatusCount(), 1);
  }

  @Test
  public void testGetSuccessInitializingAndPassing() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setInitializingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType(AssertionType.FRESHNESS.name())
                    .setSource("test")
                    .setLastResultAt(100L))));
    summary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN_2))
                    .setType(AssertionType.VOLUME.name())
                    .setSource("test")
                    .setLastResultAt(0L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    // INIT takes precedence over PASS
    assertEquals(result.get(0).getStatus(), HealthStatus.INIT);
    assertEquals(result.get(0).getMessage(), "1 of 2 assertions are initializing");

    // Verify latestAssertionStatusByType includes both types
    List<AssertionHealthStatusByType> assertionStatusByType =
        result.get(0).getLatestAssertionStatusByType();
    assertNotNull(assertionStatusByType);
    assertEquals(assertionStatusByType.size(), 2);
  }

  @Test
  public void testGetSuccessFailTakesPrecedenceOverInit() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType(AssertionType.FRESHNESS.name())
                    .setSource("test")
                    .setLastResultAt(100L))));
    summary.setInitializingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN_2))
                    .setType(AssertionType.VOLUME.name())
                    .setSource("test")
                    .setLastResultAt(50L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    // FAIL should take precedence over INIT
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getMessage(), "1 of 2 assertions are failing");
  }

  @Test
  public void testGetSuccessErrorTakesPrecedenceOverInit() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setErroringAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType(AssertionType.FRESHNESS.name())
                    .setSource("test")
                    .setLastResultAt(100L))));
    summary.setInitializingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN_2))
                    .setType(AssertionType.VOLUME.name())
                    .setSource("test")
                    .setLastResultAt(50L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    // WARN should take precedence over INIT
    assertEquals(result.get(0).getStatus(), HealthStatus.WARN);
    assertEquals(result.get(0).getMessage(), "1 of 2 assertions are erroring");
  }

  @Test
  public void testGetSuccessNullHealthMissingAspectFields() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetSuccessNullHealthEmptyAspectFields() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray());
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray());

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetSuccessUnhealthyFailingOnly() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType("type")
                    .setSource("test")
                    .setLastResultAt(0L))));

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    System.out.println(result);
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getReportedAt(), 0L);
  }

  @Test
  public void testGetSuccessUnhealthyFailingPassingEmpty() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylEntityHealthResolver resolver =
        new AcrylEntityHealthResolver(
            entityClient, new AcrylEntityHealthResolver.Config(true, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
                    .setType("type")
                    .setSource("test")
                    .setLastResultAt(0L))));
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray());

    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(summary.data()))))));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
  }
}
