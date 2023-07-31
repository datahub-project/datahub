package com.linkedin.datahub.graphql.resolvers.dataset;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AcrylDatasetHealthResolverTest {

  private static final String TEST_DATASET_URN = "urn:li:dataset:(test,test,test)";
  private static final String TEST_ASSERTION_URN = "urn:li:assertion:test-guid";
  private static final String TEST_ASSERTION_URN_2 = "urn:li:assertion:test-guid-2";


  @Test
  public void testGetSuccessHealthyPassingOnly() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
        new AssertionSummaryDetails()
          .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
          .setType("type")
          .setSource("test")
          .setLastResultAt(0L)))
    );

    Mockito.when(entityClient.getV2(
      Mockito.eq(Constants.DATASET_ENTITY_NAME),
      Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
      Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
      Mockito.any()
    ))
    .thenReturn(new EntityResponse()
        .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
        .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
            Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
            new EnvelopedAspect()
               .setValue(new Aspect(summary.data()))
        )))
    );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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
  public void testGetSuccessHealthyPassingEmptyFailing() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
        new AssertionSummaryDetails()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
            .setType("type")
            .setSource("test")
            .setLastResultAt(0L)))
    );
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray());

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    ))
        .thenReturn(new EntityResponse()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect()
                    .setValue(new Aspect(summary.data()))
            )))
        );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    )).thenReturn(null);


    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetSuccessNullHealthMissingAspectFields() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    ))
        .thenReturn(new EntityResponse()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect()
                    .setValue(new Aspect(summary.data()))
            )))
        );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray());
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray());

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    ))
        .thenReturn(new EntityResponse()
            .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect()
                    .setValue(new Aspect(summary.data()))
            )))
        );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
        new AssertionSummaryDetails()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
            .setType("type")
            .setSource("test")
            .setLastResultAt(0L)))
    );

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    ))
        .thenReturn(new EntityResponse()
            .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect()
                    .setValue(new Aspect(summary.data()))
            )))
        );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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
  }

  @Test
  public void testGetSuccessUnhealthyFailingPassingEmpty() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
        new AssertionSummaryDetails()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
            .setType("type")
            .setSource("test")
            .setLastResultAt(0L)))
    );
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray());

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    ))
        .thenReturn(new EntityResponse()
            .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect()
                    .setValue(new Aspect(summary.data()))
            )))
        );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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


  @Test
  public void testGetSuccessUnhealthyFailingAndPassing() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    AcrylDatasetHealthResolver resolver = new AcrylDatasetHealthResolver(entityClient,
        new AcrylDatasetHealthResolver.Config(true, false, false));

    AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
        new AssertionSummaryDetails()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
            .setType("type")
            .setSource("test")
            .setLastResultAt(0L)))
    );
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
        new AssertionSummaryDetails()
            .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN_2))
            .setType("type")
            .setSource("test")
            .setLastResultAt(0L)))
    );

    Mockito.when(entityClient.getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
        Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)),
        Mockito.any()
    ))
        .thenReturn(new EntityResponse()
            .setUrn(UrnUtils.getUrn(TEST_DATASET_URN))
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.ASSERTIONS_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect()
                    .setValue(new Aspect(summary.data()))
            )))
        );

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
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
