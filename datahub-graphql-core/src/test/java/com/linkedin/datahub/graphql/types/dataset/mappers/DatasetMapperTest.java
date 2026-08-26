package com.linkedin.datahub.graphql.types.dataset.mappers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetMapperTest {

  private static final Urn TEST_DATASET_URN =
      Urn.createFromTuple(Constants.DATASET_ENTITY_NAME, "test");
  private static final Urn TEST_CREATED_ACTOR_URN =
      Urn.createFromTuple(Constants.CORP_USER_ENTITY_NAME, "created");
  private static final Urn TEST_LAST_MODIFIED_ACTOR_URN =
      Urn.createFromTuple(Constants.CORP_USER_ENTITY_NAME, "lastmodified");
  private static final Urn TEST_PARENT_URN =
      Urn.createFromTuple(Constants.DATASET_ENTITY_NAME, "parent");
  private static final Urn TEST_ACTOR_URN =
      Urn.createFromTuple(Constants.CORP_USER_ENTITY_NAME, "actor");

  @Test
  public void testDatasetPropertiesMapperWithCreatedAndLastModified() {
    final com.linkedin.dataset.DatasetProperties input =
        new com.linkedin.dataset.DatasetProperties();
    input.setName("Test");
    input.setQualifiedName("Test QualifiedName");

    final TimeStamp createdTimestamp = new TimeStamp();
    createdTimestamp.setActor(TEST_CREATED_ACTOR_URN);
    createdTimestamp.setTime(10L);
    input.setCreated(createdTimestamp);

    final TimeStamp lastModifiedTimestamp = new TimeStamp();
    lastModifiedTimestamp.setActor(TEST_LAST_MODIFIED_ACTOR_URN);
    lastModifiedTimestamp.setTime(20L);
    input.setLastModified(lastModifiedTimestamp);

    final Map<String, com.linkedin.entity.EnvelopedAspect> dataSetPropertiesAspects =
        new HashMap<>();
    dataSetPropertiesAspects.put(
        Constants.DATASET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(dataSetPropertiesAspects));
    final Dataset actual = DatasetMapper.map(null, response);

    final Dataset expected = new Dataset();
    expected.setUrn(TEST_DATASET_URN.toString());
    final DatasetProperties expectedDatasetProperties = new DatasetProperties();
    expectedDatasetProperties.setName("Test");
    expectedDatasetProperties.setQualifiedName("Test QualifiedName");
    expectedDatasetProperties.setLastModifiedActor(TEST_LAST_MODIFIED_ACTOR_URN.toString());
    expectedDatasetProperties.setCreatedActor(TEST_CREATED_ACTOR_URN.toString());
    expectedDatasetProperties.setLastModified(
        new com.linkedin.datahub.graphql.generated.AuditStamp(
            20L, TEST_LAST_MODIFIED_ACTOR_URN.toString()));
    expectedDatasetProperties.setCreated(10L);
    expected.setProperties(expectedDatasetProperties);

    Assert.assertEquals(actual.getUrn(), expected.getUrn());
    Assert.assertEquals(actual.getProperties().getName(), expected.getProperties().getName());
    Assert.assertEquals(
        actual.getProperties().getQualifiedName(), expected.getProperties().getQualifiedName());

    Assert.assertEquals(
        actual.getProperties().getLastModified().getTime(),
        expected.getProperties().getLastModified().getTime());
    Assert.assertEquals(
        actual.getProperties().getLastModified().getActor(),
        expected.getProperties().getLastModified().getActor());
    Assert.assertEquals(actual.getProperties().getCreated(), expected.getProperties().getCreated());

    Assert.assertEquals(
        actual.getProperties().getLastModifiedActor(),
        expected.getProperties().getLastModifiedActor());
    Assert.assertEquals(
        actual.getProperties().getCreatedActor(), expected.getProperties().getCreatedActor());
  }

  @Test
  public void testDatasetPropertiesMapperWithoutCreatedAndLastModified() {
    final com.linkedin.dataset.DatasetProperties input =
        new com.linkedin.dataset.DatasetProperties();
    input.setName("Test");

    final Map<String, com.linkedin.entity.EnvelopedAspect> dataSetPropertiesAspects =
        new HashMap<>();
    dataSetPropertiesAspects.put(
        Constants.DATASET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(dataSetPropertiesAspects));
    final Dataset actual = DatasetMapper.map(null, response);

    final Dataset expected = new Dataset();
    expected.setUrn(TEST_DATASET_URN.toString());
    final DatasetProperties expectedDatasetProperties = new DatasetProperties();
    expectedDatasetProperties.setName("Test");
    expectedDatasetProperties.setLastModifiedActor(null);
    expectedDatasetProperties.setCreatedActor(null);
    expectedDatasetProperties.setLastModified(
        new com.linkedin.datahub.graphql.generated.AuditStamp(0L, null));
    expectedDatasetProperties.setCreated(null);
    expected.setProperties(expectedDatasetProperties);

    Assert.assertEquals(actual.getUrn(), expected.getUrn());
    Assert.assertEquals(actual.getProperties().getName(), expected.getProperties().getName());

    Assert.assertEquals(
        actual.getProperties().getLastModified().getTime(),
        expected.getProperties().getLastModified().getTime());
    Assert.assertEquals(
        actual.getProperties().getLastModified().getActor(),
        expected.getProperties().getLastModified().getActor());
    Assert.assertEquals(actual.getProperties().getCreated(), expected.getProperties().getCreated());

    Assert.assertEquals(
        actual.getProperties().getLastModifiedActor(),
        expected.getProperties().getLastModifiedActor());
    Assert.assertEquals(
        actual.getProperties().getCreatedActor(), expected.getProperties().getCreatedActor());
  }

  @Test
  public void testDatasetPropertiesMapperWithoutTimestampActors() {
    final com.linkedin.dataset.DatasetProperties input =
        new com.linkedin.dataset.DatasetProperties();
    input.setName("Test");

    TimeStamp createdTimestamp = new TimeStamp();
    createdTimestamp.setTime(10L);
    input.setCreated(createdTimestamp);

    TimeStamp lastModifiedTimestamp = new TimeStamp();
    lastModifiedTimestamp.setTime(20L);
    input.setLastModified(lastModifiedTimestamp);

    final Map<String, com.linkedin.entity.EnvelopedAspect> dataSetPropertiesAspects =
        new HashMap<>();
    dataSetPropertiesAspects.put(
        Constants.DATASET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(dataSetPropertiesAspects));
    final Dataset actual = DatasetMapper.map(null, response);

    final Dataset expected = new Dataset();
    expected.setUrn(TEST_DATASET_URN.toString());
    final DatasetProperties expectedDatasetProperties = new DatasetProperties();
    expectedDatasetProperties.setName("Test");
    expectedDatasetProperties.setLastModifiedActor(null);
    expectedDatasetProperties.setCreatedActor(null);
    expectedDatasetProperties.setLastModified(
        new com.linkedin.datahub.graphql.generated.AuditStamp(20L, null));
    expectedDatasetProperties.setCreated(10L);
    expected.setProperties(expectedDatasetProperties);

    Assert.assertEquals(actual.getUrn(), expected.getUrn());
    Assert.assertEquals(actual.getProperties().getName(), expected.getProperties().getName());

    Assert.assertEquals(
        actual.getProperties().getLastModified().getTime(),
        expected.getProperties().getLastModified().getTime());
    Assert.assertEquals(
        actual.getProperties().getLastModified().getActor(),
        expected.getProperties().getLastModified().getActor());
    Assert.assertEquals(actual.getProperties().getCreated(), expected.getProperties().getCreated());

    Assert.assertEquals(
        actual.getProperties().getLastModifiedActor(),
        expected.getProperties().getLastModifiedActor());
    Assert.assertEquals(
        actual.getProperties().getCreatedActor(), expected.getProperties().getCreatedActor());
  }

  @Test
  public void testDatasetMapperWithLogicalParent() {
    final LogicalParent input = new LogicalParent();
    final Edge edge = new Edge();
    edge.setDestinationUrn(TEST_PARENT_URN);
    edge.setCreated(new AuditStamp().setTime(10L).setActor(TEST_ACTOR_URN));
    edge.setLastModified(new AuditStamp().setTime(20L).setActor(TEST_ACTOR_URN));
    input.setParent(edge);

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.LOGICAL_PARENT_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));

    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getLogicalParent());
    Assert.assertEquals(actual.getLogicalParent().getUrn(), TEST_PARENT_URN.toString());
  }

  @Test
  public void testDatasetMapperWithNullLogicalParent() {
    final LogicalParent input = new LogicalParent();
    // Don't set parent - leave it as default (null)

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.LOGICAL_PARENT_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));

    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(null, response);

    Assert.assertNull(actual.getLogicalParent());
  }

  @Test
  public void testDatasetMapperWithoutLogicalParent() {
    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();

    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(null, response);

    Assert.assertNull(actual.getLogicalParent());
  }

  @Test
  public void testDatasetMapperWithSemanticModelProperties() {
    final Urn semanticModelUrn =
        Urn.createFromTuple(Constants.SEMANTIC_MODEL_ENTITY_NAME, "dbt", "analytics.orders", "m");
    final com.linkedin.dataset.SemanticModelProperties input =
        new com.linkedin.dataset.SemanticModelProperties()
            .setAlias("orders_ds")
            .setSemanticModel(semanticModelUrn);

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.SEMANTIC_MODEL_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));

    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getSemanticModelProperties());
    Assert.assertEquals(actual.getSemanticModelProperties().getAlias(), "orders_ds");
    Assert.assertNotNull(actual.getSemanticModelProperties().getSemanticModel());
    Assert.assertEquals(
        actual.getSemanticModelProperties().getSemanticModel().getUrn(),
        semanticModelUrn.toString());
  }

  @Test
  public void testDatasetMapperViewPropertiesWithFormattedLogic() {
    final com.linkedin.dataset.ViewProperties input = new com.linkedin.dataset.ViewProperties();
    input.setMaterialized(true);
    input.setViewLanguage("SQL");
    input.setViewLogic("select * from {{ ref('upstream') }}");
    input.setFormattedViewLogic("select * from warehouse.schema.upstream");

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertTrue(actual.getViewProperties().getMaterialized());
    Assert.assertEquals(actual.getViewProperties().getLanguage(), "SQL");
    Assert.assertEquals(
        actual.getViewProperties().getLogic(), "select * from {{ ref('upstream') }}");
    Assert.assertEquals(
        actual.getViewProperties().getFormattedLogic(), "select * from warehouse.schema.upstream");
  }

  @Test
  public void testDatasetMapperViewPropertiesWithoutFormattedLogic() {
    final com.linkedin.dataset.ViewProperties input = new com.linkedin.dataset.ViewProperties();
    input.setMaterialized(false);
    input.setViewLanguage("SQL");
    input.setViewLogic("select 1");

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertEquals(actual.getViewProperties().getLogic(), "select 1");
    Assert.assertNull(actual.getViewProperties().getFormattedLogic());
  }

  /**
   * Regression test for the gap Cursor Bugbot flagged on PR #16319: {@code viewProperties.logic}
   * was populated unconditionally, so revoking {@code VIEW_ENTITY_QUERIES} hid the SQL in the UI
   * but not from a raw GraphQL read. An actor lacking the privilege (but who can otherwise view the
   * dataset) must get {@code logic}/{@code formattedLogic} withheld; {@code materialized}/{@code
   * language} are non-sensitive and remain visible either way.
   */
  @Test
  public void testDatasetMapperViewPropertiesWithheldWithoutViewEntityQueries() {
    final com.linkedin.dataset.ViewProperties input = new com.linkedin.dataset.ViewProperties();
    input.setMaterialized(true);
    input.setViewLanguage("SQL");
    input.setViewLogic("select * from secret_table");
    input.setFormattedViewLogic("select * from warehouse.schema.secret_table");

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(denyAllQueryContext(), response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertTrue(actual.getViewProperties().getMaterialized());
    Assert.assertEquals(actual.getViewProperties().getLanguage(), "SQL");
    Assert.assertNull(
        actual.getViewProperties().getLogic(),
        "logic leaked to an actor lacking VIEW_ENTITY_QUERIES");
    Assert.assertNull(
        actual.getViewProperties().getFormattedLogic(),
        "formattedLogic leaked to an actor lacking VIEW_ENTITY_QUERIES");
  }

  /** Mirror allow-case: an actor granted the privilege still sees the SQL. */
  @Test
  public void testDatasetMapperViewPropertiesShownWithViewEntityQueries() {
    final com.linkedin.dataset.ViewProperties input = new com.linkedin.dataset.ViewProperties();
    input.setMaterialized(true);
    input.setViewLanguage("SQL");
    input.setViewLogic("select * from allowed_table");

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dataset actual = DatasetMapper.map(allowAllQueryContext(), response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertEquals(actual.getViewProperties().getLogic(), "select * from allowed_table");
  }

  private static QueryContext denyAllQueryContext() {
    Authorizer denyAuthorizer = mock(Authorizer.class);
    AuthorizationResult denyResult = mock(AuthorizationResult.class);
    when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(denyAuthorizer.authorize(any())).thenReturn(denyResult);
    return queryContextWithAuthorizer(denyAuthorizer);
  }

  private static QueryContext allowAllQueryContext() {
    Authorizer allowAuthorizer = mock(Authorizer.class);
    AuthorizationResult allowResult = mock(AuthorizationResult.class);
    when(allowResult.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(allowAuthorizer.authorize(any())).thenReturn(allowResult);
    return queryContextWithAuthorizer(allowAuthorizer);
  }

  private static QueryContext queryContextWithAuthorizer(Authorizer authorizer) {
    final String actorUrn = "urn:li:corpuser:test";
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");
    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(authorizer, authentication);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);
    when(mockContext.getOperationContext()).thenReturn(operationContext);
    return mockContext;
  }
}
