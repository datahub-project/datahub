package com.linkedin.datahub.graphql.types.metric.mappers;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Metric;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MetricKey;
import com.linkedin.metric.DerivedMetricInput;
import com.linkedin.metric.DerivedMetricInputArray;
import com.linkedin.metric.MetricInfo;
import com.linkedin.metric.MetricRelationships;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetricMapperTest {

  private static final String PLATFORM_URN = "urn:li:dataPlatform:dbt";
  private static final String METRIC_URN =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,revenue)";
  private static final String SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.orders_model,my_model)";
  private static final String ACTOR_URN = "urn:li:corpuser:testuser";
  private static final String PARENT_METRIC_URN =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,gross_revenue)";
  private static final String DERIVED_METRIC_URN_1 =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,subtotal)";
  private static final String DERIVED_METRIC_URN_2 =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,tax)";
  private static final Long TEST_TIMESTAMP = 1640995200000L;

  private Urn metricUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    metricUrn = Urn.createFromString(METRIC_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapKeyOnly() {
    EntityResponse entityResponse = createBaseEntityResponse();

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(metricUrn))).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), METRIC_URN);
      assertEquals(result.getType(), EntityType.METRIC);
      assertNotNull(result.getPlatform());
      assertEquals(result.getPlatform().getUrn(), PLATFORM_URN);
      assertEquals(result.getPath(), "analytics.orders_model");
      assertEquals(result.getId(), "revenue");
    }
  }

  @Test
  public void testMapMetricInfoAuditStamps() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    Urn actorUrn = Urn.createFromString(ACTOR_URN);
    AuditStamp createdStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn);
    AuditStamp lastModifiedStamp =
        new AuditStamp().setTime(TEST_TIMESTAMP + 1000L).setActor(actorUrn);

    MetricInfo info = new MetricInfo().setName("Revenue");
    info.setCreated(createdStamp);
    info.setLastModified(lastModifiedStamp);
    addAspect(entityResponse, METRIC_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(metricUrn))).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo());
      assertEquals(result.getInfo().getName(), "Revenue");
      assertNotNull(result.getInfo().getCreated());
      assertEquals(result.getInfo().getCreated().getTime(), TEST_TIMESTAMP);
      assertEquals(result.getInfo().getCreated().getActor().getUrn(), ACTOR_URN);
      assertNotNull(result.getInfo().getLastModified());
      assertEquals(result.getInfo().getLastModified().getTime(), TEST_TIMESTAMP + 1000L);
    }
  }

  @Test
  public void testMapMetricInfoSemanticModelStub() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    MetricInfo info =
        new MetricInfo()
            .setName("Revenue")
            .setSemanticModel(Urn.createFromString(SEMANTIC_MODEL_URN));
    addAspect(entityResponse, METRIC_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(metricUrn))).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getSemanticModel());
      assertEquals(result.getSemanticModel().getUrn(), SEMANTIC_MODEL_URN);
      assertEquals(result.getSemanticModel().getType(), EntityType.SEMANTIC_MODEL);
    }
  }

  @Test
  public void testTopLevelParentMetricPopulatedFromRelationshipsAspect() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    MetricRelationships rels =
        new MetricRelationships().setParentMetric(Urn.createFromString(PARENT_METRIC_URN));
    addAspect(entityResponse, METRIC_RELATIONSHIPS_ASPECT_NAME, rels);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getParentMetric());
      assertEquals(result.getParentMetric().getUrn(), PARENT_METRIC_URN);
      assertEquals(result.getParentMetric().getType(), EntityType.METRIC);
    }
  }

  @Test
  public void testMapMetricRelationshipsDerivedFrom() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    Urn actorUrn = Urn.createFromString(ACTOR_URN);
    AuditStamp createdStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn);

    StringMap props1 = new StringMap();
    props1.put("weight", "0.7");

    DerivedMetricInput input1 =
        new DerivedMetricInput()
            .setDestinationUrn(Urn.createFromString(DERIVED_METRIC_URN_1))
            .setCreated(createdStamp)
            .setProperties(props1);

    DerivedMetricInput input2 =
        new DerivedMetricInput().setDestinationUrn(Urn.createFromString(DERIVED_METRIC_URN_2));

    MetricRelationships rels =
        new MetricRelationships().setDerivedFrom(new DerivedMetricInputArray(input1, input2));
    addAspect(entityResponse, METRIC_RELATIONSHIPS_ASPECT_NAME, rels);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getMetricRelationships());
      assertEquals(result.getMetricRelationships().getDerivedFrom().size(), 2);
      assertEquals(
          result.getMetricRelationships().getDerivedFrom().get(0).getDestination().getUrn(),
          DERIVED_METRIC_URN_1);
      assertNotNull(result.getMetricRelationships().getDerivedFrom().get(0).getCreated());
      assertEquals(
          result.getMetricRelationships().getDerivedFrom().get(0).getCreated().getTime(),
          TEST_TIMESTAMP);
      assertNotNull(result.getMetricRelationships().getDerivedFrom().get(0).getProperties());
      assertEquals(
          result.getMetricRelationships().getDerivedFrom().get(0).getProperties().get(0).getKey(),
          "weight");
      assertEquals(
          result.getMetricRelationships().getDerivedFrom().get(0).getProperties().get(0).getValue(),
          "0.7");
      assertEquals(
          result.getMetricRelationships().getDerivedFrom().get(1).getDestination().getUrn(),
          DERIVED_METRIC_URN_2);
    }
  }

  @Test
  public void testMapMetricRelationshipsRelatedMetricsDefaultEmpty() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    MetricRelationships rels = new MetricRelationships();
    addAspect(entityResponse, METRIC_RELATIONSHIPS_ASPECT_NAME, rels);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(metricUrn))).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getMetricRelationships());
      assertNotNull(result.getMetricRelationships().getRelatedMetrics());
      assertTrue(result.getMetricRelationships().getRelatedMetrics().isEmpty());
    }
  }

  @Test
  public void testMapAbsentMetricUpstreams() {
    EntityResponse entityResponse = createBaseEntityResponse();

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(metricUrn))).thenReturn(true);

      Metric result = MetricMapper.map(mockQueryContext, entityResponse);

      assertNull(result.getMetricUpstreams());
    }
  }

  private EntityResponse createBaseEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(metricUrn);

    try {
      MetricKey key = new MetricKey();
      key.setPlatform(Urn.createFromString(PLATFORM_URN));
      key.setPath("analytics.orders_model");
      key.setId("revenue");

      EnvelopedAspect keyAspect = new EnvelopedAspect();
      keyAspect.setValue(new Aspect(key.data()));

      Map<String, EnvelopedAspect> aspects = new HashMap<>();
      aspects.put(METRIC_KEY_ASPECT_NAME, keyAspect);
      entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return entityResponse;
  }

  private void addAspect(
      EntityResponse entityResponse, String aspectName, RecordTemplate aspectData) {
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(aspectData.data()));
    entityResponse.getAspects().put(aspectName, aspect);
  }
}
