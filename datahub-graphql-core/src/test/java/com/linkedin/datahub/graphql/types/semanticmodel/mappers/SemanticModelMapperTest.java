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
import com.linkedin.metric.DialectExpression;
import com.linkedin.metric.DialectExpressionArray;
import com.linkedin.metric.MetricExpression;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import com.linkedin.semanticmodel.Dimension;
import com.linkedin.semanticmodel.ModelDataset;
import com.linkedin.semanticmodel.ModelDatasetArray;
import com.linkedin.semanticmodel.SemanticField;
import com.linkedin.semanticmodel.SemanticFieldArray;
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
      "urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.my_table,PROD)";
  private static final String QUERY_URN = "urn:li:query:abc123";
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
  public void testMapDatasetsSourceDataset() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    ModelDataset modelDataset =
        new ModelDataset().setName("orders_ds").setSource(Urn.createFromString(DATASET_URN));
    SemanticModelInfo info =
        new SemanticModelInfo()
            .setName("My Model")
            .setDatasets(new ModelDatasetArray(modelDataset));
    addAspect(entityResponse, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo());
      assertNotNull(result.getInfo().getDatasets());
      assertEquals(result.getInfo().getDatasets().size(), 1);
      assertEquals(result.getInfo().getDatasets().get(0).getName(), "orders_ds");
      assertNotNull(result.getInfo().getDatasets().get(0).getSource());
      assertEquals(result.getInfo().getDatasets().get(0).getSource().getUrn(), DATASET_URN);
      assertEquals(result.getInfo().getDatasets().get(0).getSource().getType(), EntityType.DATASET);
    }
  }

  @Test
  public void testMapDatasetsSourceQuery() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    ModelDataset modelDataset =
        new ModelDataset().setName("inline_query_ds").setSource(Urn.createFromString(QUERY_URN));
    SemanticModelInfo info =
        new SemanticModelInfo()
            .setName("My Model")
            .setDatasets(new ModelDatasetArray(modelDataset));
    addAspect(entityResponse, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo().getDatasets().get(0).getSource());
      assertEquals(result.getInfo().getDatasets().get(0).getSource().getUrn(), QUERY_URN);
      assertEquals(result.getInfo().getDatasets().get(0).getSource().getType(), EntityType.QUERY);
    }
  }

  @Test
  public void testMapSemanticFieldSchemaFieldAndUrn() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    SchemaField schemaField =
        new SchemaField()
            .setFieldPath("revenue")
            .setNativeDataType("NUMERIC")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setDescription("Revenue dimension");

    MetricExpression expression =
        new MetricExpression()
            .setDialects(
                new DialectExpressionArray(
                    new DialectExpression()
                        .setDialect(com.linkedin.metric.Dialect.ANSI_SQL)
                        .setExpression("SUM(revenue)")));

    SemanticField semanticField =
        new SemanticField()
            .setSchemaField(schemaField)
            .setType(com.linkedin.semanticmodel.SemanticFieldType.MEASURE)
            .setExpression(expression);

    ModelDataset modelDataset =
        new ModelDataset()
            .setName("orders_ds")
            .setSource(Urn.createFromString(DATASET_URN))
            .setFields(new SemanticFieldArray(semanticField));
    SemanticModelInfo info =
        new SemanticModelInfo()
            .setName("My Model")
            .setDatasets(new ModelDatasetArray(modelDataset));
    addAspect(entityResponse, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getInfo().getDatasets().get(0).getFields());
      assertEquals(result.getInfo().getDatasets().get(0).getFields().size(), 1);

      com.linkedin.datahub.graphql.generated.SemanticField field =
          result.getInfo().getDatasets().get(0).getFields().get(0);
      assertNotNull(field.getSchemaField());
      assertEquals(field.getSchemaField().getFieldPath(), "revenue");
      assertEquals(field.getSchemaField().getDescription(), "Revenue dimension");
      assertEquals(
          field.getSchemaField().getType(),
          com.linkedin.datahub.graphql.generated.SchemaFieldDataType.STRING);
      assertNotNull(field.getSchemaField().getSchemaFieldEntity());
      String expectedSchemaFieldUrn = "urn:li:schemaField:(" + SEMANTIC_MODEL_URN + ",revenue)";
      assertEquals(field.getSchemaField().getSchemaFieldEntity().getUrn(), expectedSchemaFieldUrn);
      assertEquals(
          field.getType(), com.linkedin.datahub.graphql.generated.SemanticFieldType.MEASURE);
    }
  }

  @Test
  public void testMapSemanticFieldTypeDiscriminatorAllValues() throws URISyntaxException {
    EntityResponse entityResponse = createBaseEntityResponse();

    MetricExpression expr =
        new MetricExpression()
            .setDialects(
                new DialectExpressionArray(
                    new DialectExpression()
                        .setDialect(com.linkedin.metric.Dialect.ANSI_SQL)
                        .setExpression("1")));

    SemanticField dimensionField =
        new SemanticField()
            .setSchemaField(buildSchemaField("dim_field"))
            .setType(com.linkedin.semanticmodel.SemanticFieldType.DIMENSION)
            .setExpression(expr)
            .setDimension(new Dimension().setIsTime(true));

    SemanticField measureField =
        new SemanticField()
            .setSchemaField(buildSchemaField("measure_field"))
            .setType(com.linkedin.semanticmodel.SemanticFieldType.MEASURE)
            .setExpression(expr);

    SemanticField filterField =
        new SemanticField()
            .setSchemaField(buildSchemaField("filter_field"))
            .setType(com.linkedin.semanticmodel.SemanticFieldType.FILTER)
            .setExpression(expr);

    SemanticField otherField =
        new SemanticField()
            .setSchemaField(buildSchemaField("other_field"))
            .setType(com.linkedin.semanticmodel.SemanticFieldType.OTHER)
            .setExpression(expr);

    ModelDataset modelDataset =
        new ModelDataset()
            .setName("ds")
            .setSource(Urn.createFromString(DATASET_URN))
            .setFields(
                new SemanticFieldArray(dimensionField, measureField, filterField, otherField));
    addAspect(
        entityResponse,
        SEMANTIC_MODEL_INFO_ASPECT_NAME,
        new SemanticModelInfo().setName("M").setDatasets(new ModelDatasetArray(modelDataset)));

    try (MockedStatic<AuthorizationUtils> authMock = mockStatic(AuthorizationUtils.class)) {
      authMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      SemanticModel result = SemanticModelMapper.map(mockQueryContext, entityResponse);
      java.util.List<com.linkedin.datahub.graphql.generated.SemanticField> fields =
          result.getInfo().getDatasets().get(0).getFields();

      assertEquals(
          fields.get(0).getType(),
          com.linkedin.datahub.graphql.generated.SemanticFieldType.DIMENSION);
      assertNotNull(fields.get(0).getDimension());
      assertTrue(fields.get(0).getDimension().getIsTime());

      assertEquals(
          fields.get(1).getType(),
          com.linkedin.datahub.graphql.generated.SemanticFieldType.MEASURE);
      assertNull(fields.get(1).getDimension());

      assertEquals(
          fields.get(2).getType(), com.linkedin.datahub.graphql.generated.SemanticFieldType.FILTER);
      assertNull(fields.get(2).getDimension());

      assertEquals(
          fields.get(3).getType(), com.linkedin.datahub.graphql.generated.SemanticFieldType.OTHER);
      assertNull(fields.get(3).getDimension());
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

  private SchemaField buildSchemaField(String fieldPath) {
    return new SchemaField()
        .setFieldPath(fieldPath)
        .setNativeDataType("string")
        .setType(
            new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
  }
}
