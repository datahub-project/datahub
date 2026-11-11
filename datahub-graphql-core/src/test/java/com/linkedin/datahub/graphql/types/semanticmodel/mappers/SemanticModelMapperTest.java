package com.linkedin.datahub.graphql.types.semanticmodel.mappers;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipCardinality;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.SemanticModelKey;
import com.linkedin.semanticmodel.SemanticModelInfo;
import com.linkedin.semanticmodel.SemanticModelRelationship;
import com.linkedin.semanticmodel.SemanticModelRelationshipArray;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SemanticModelMapperTest {

  private static final String PLATFORM_URN = "urn:li:dataPlatform:dbt";
  private static final String SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.orders_model,my_model)";
  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.orders_ds,PROD)";
  private static final String ACTOR_URN = "urn:li:corpuser:testuser";
  private static final Long TEST_TIMESTAMP = 1640995200000L;

  private Urn semanticModelUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    semanticModelUrn = Urn.createFromString(SEMANTIC_MODEL_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapKeyOnly() {
    EntityResponse entityResponse = createBaseEntityResponse();

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(semanticModelUrn))).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), SEMANTIC_MODEL_URN);
      assertEquals(result.getType(), EntityType.SEMANTIC_MODEL);
      assertNotNull(result.getPlatform());
      assertEquals(result.getPlatform().getUrn(), PLATFORM_URN);
      assertEquals(result.getPath(), "analytics.orders_model");
      assertEquals(result.getId(), "my_model");
    }
  }

  @Test
  public void testMapSemanticModelInfoAuditStamps() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    Urn actorUrn = Urn.createFromString(ACTOR_URN);
    AuditStamp createdStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn);
    AuditStamp lastModifiedStamp =
        new AuditStamp().setTime(TEST_TIMESTAMP + 1000L).setActor(actorUrn);

    SemanticModelInfo info = new SemanticModelInfo().setName("My Semantic Model");
    info.setCreated(createdStamp);
    info.setLastModified(lastModifiedStamp);
    addAspect(entityResponse, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(semanticModelUrn))).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo());
      assertEquals(result.getInfo().getName(), "My Semantic Model");
      assertNotNull(result.getInfo().getCreated());
      assertEquals(result.getInfo().getCreated().getTime(), TEST_TIMESTAMP);
      assertEquals(result.getInfo().getCreated().getActor().getUrn(), ACTOR_URN);
      assertNotNull(result.getInfo().getLastModified());
      assertEquals(result.getInfo().getLastModified().getTime(), TEST_TIMESTAMP + 1000L);
    }
  }

  @Test
  public void testMapDatasetsAbsentIsEmptyList() {
    EntityResponse entityResponse = createBaseEntityResponse();

    SemanticModelInfo info = new SemanticModelInfo().setName("My Model");
    addAspect(entityResponse, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo().getDatasets());
      assertTrue(result.getInfo().getDatasets().isEmpty());
    }
  }

  @Test
  public void testMapRelationshipsCardinality() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    SemanticModelRelationship rel =
        new SemanticModelRelationship()
            .setName("orders_to_customers")
            .setFrom("orders_ds")
            .setFromColumns(new com.linkedin.data.template.StringArray("order_id"))
            .setTo("customers_ds")
            .setToColumns(new com.linkedin.data.template.StringArray("customer_id"))
            .setCardinality(com.linkedin.ermodelrelation.ERModelRelationshipCardinality.N_ONE);

    SemanticModelRelationship relNoCardinality =
        new SemanticModelRelationship()
            .setFrom("a")
            .setFromColumns(new com.linkedin.data.template.StringArray("x"))
            .setTo("b")
            .setToColumns(new com.linkedin.data.template.StringArray("y"));

    SemanticModelInfo info =
        new SemanticModelInfo()
            .setName("M")
            .setRelationships(new SemanticModelRelationshipArray(rel, relNoCardinality));
    addAspect(entityResponse, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(semanticModelUrn))).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo().getRelationships());
      assertEquals(result.getInfo().getRelationships().size(), 2);

      com.linkedin.datahub.graphql.generated.SemanticModelRelationship mapped0 =
          result.getInfo().getRelationships().get(0);
      assertEquals(mapped0.getName(), "orders_to_customers");
      assertEquals(mapped0.getFrom(), "orders_ds");
      assertEquals(mapped0.getFromColumns().get(0), "order_id");
      assertEquals(mapped0.getTo(), "customers_ds");
      assertEquals(mapped0.getToColumns().get(0), "customer_id");
      assertEquals(mapped0.getCardinality(), ERModelRelationshipCardinality.N_ONE);

      assertNull(result.getInfo().getRelationships().get(1).getCardinality());
    }
  }

  @Test
  public void testMapFineGrainedLineages() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    Urn upstreamFieldUrn = Urn.createFromString("urn:li:schemaField:(" + DATASET_URN + ",revenue)");
    Urn downstreamFieldUrn =
        Urn.createFromString("urn:li:schemaField:(" + SEMANTIC_MODEL_URN + ",total_revenue)");

    UrnArray upstreamUrns = new UrnArray(upstreamFieldUrn);
    UrnArray downstreamUrns = new UrnArray(downstreamFieldUrn);

    FineGrainedLineage fgl = new FineGrainedLineage(new DataMap());
    fgl.setUpstreams(upstreamUrns);
    fgl.setDownstreams(downstreamUrns);
    fgl.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fgl.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);

    UpstreamLineage upstreamLineage =
        new UpstreamLineage().setFineGrainedLineages(new FineGrainedLineageArray(fgl));
    addAspect(entityResponse, UPSTREAM_LINEAGE_ASPECT_NAME, upstreamLineage);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(semanticModelUrn))).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getFineGrainedLineages());
      assertEquals(result.getFineGrainedLineages().size(), 1);
    }
  }

  @Test
  public void testMapAbsentUpstreamLineage() {
    EntityResponse entityResponse = createBaseEntityResponse();

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(semanticModelUrn))).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNull(result.getFineGrainedLineages());
    }
  }

  @Test
  public void testMapTopLevelAiContextAspect() {
    EntityResponse entityResponse = createBaseEntityResponse();

    com.linkedin.common.AiContext aiContext =
        new com.linkedin.common.AiContext()
            .setInstructions("Prefer this model for order analytics.")
            .setExamples(new com.linkedin.data.template.StringArray("orders by region"));
    addAspect(entityResponse, AI_CONTEXT_ASPECT_NAME, aiContext);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), eq(semanticModelUrn))).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getAiContext());
      assertEquals(
          result.getAiContext().getInstructions(), "Prefer this model for order analytics.");
      assertEquals(result.getAiContext().getExamples().size(), 1);
      assertEquals(result.getAiContext().getExamples().get(0), "orders by region");
    }
  }

  private EntityResponse createBaseEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(semanticModelUrn);

    try {
      SemanticModelKey key = new SemanticModelKey();
      key.setPlatform(Urn.createFromString(PLATFORM_URN));
      key.setPath("analytics.orders_model");
      key.setId("my_model");

      EnvelopedAspect keyAspect = new EnvelopedAspect();
      keyAspect.setValue(new Aspect(key.data()));

      Map<String, EnvelopedAspect> aspects = new HashMap<>();
      aspects.put(SEMANTIC_MODEL_KEY_ASPECT_NAME, keyAspect);
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
