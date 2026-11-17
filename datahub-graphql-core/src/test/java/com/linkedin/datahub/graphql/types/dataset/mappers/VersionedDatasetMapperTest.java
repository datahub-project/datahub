package com.linkedin.datahub.graphql.types.dataset.mappers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.VersionedDataset;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionedDatasetMapperTest {

  private static final Urn TEST_DATASET_URN =
      Urn.createFromTuple(Constants.DATASET_ENTITY_NAME, "test");

  @Test
  public void testVersionedDatasetMapperViewPropertiesWithFormattedLogic() {
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

    final VersionedDataset actual = VersionedDatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertTrue(actual.getViewProperties().getMaterialized());
    Assert.assertEquals(actual.getViewProperties().getLanguage(), "SQL");
    Assert.assertEquals(
        actual.getViewProperties().getLogic(), "select * from {{ ref('upstream') }}");
    Assert.assertEquals(
        actual.getViewProperties().getFormattedLogic(), "select * from warehouse.schema.upstream");
  }

  @Test
  public void testVersionedDatasetMapperViewPropertiesWithoutFormattedLogic() {
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

    final VersionedDataset actual = VersionedDatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertEquals(actual.getViewProperties().getLogic(), "select 1");
    Assert.assertNull(actual.getViewProperties().getFormattedLogic());
  }

  /**
   * Regression test for the gap Cursor Bugbot flagged on PR #16319: {@code
   * VersionedDatasetMapper.mapViewProperties} copied {@code logic}/{@code formattedLogic}
   * unconditionally even after {@code DatasetMapper}'s equivalent method was gated, so a {@code
   * versionedDataset} GraphQL read still leaked view SQL past a revoked {@code
   * VIEW_ENTITY_QUERIES}.
   */
  @Test
  public void testVersionedDatasetMapperViewPropertiesWithheldWithoutViewEntityQueries() {
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

    final VersionedDataset actual = VersionedDatasetMapper.map(denyAllQueryContext(), response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertTrue(actual.getViewProperties().getMaterialized());
    Assert.assertNull(
        actual.getViewProperties().getLogic(),
        "logic leaked to an actor lacking VIEW_ENTITY_QUERIES");
    Assert.assertNull(
        actual.getViewProperties().getFormattedLogic(),
        "formattedLogic leaked to an actor lacking VIEW_ENTITY_QUERIES");
  }

  /** Mirror allow-case: an actor granted the privilege still sees the SQL. */
  @Test
  public void testVersionedDatasetMapperViewPropertiesShownWithViewEntityQueries() {
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

    final VersionedDataset actual = VersionedDatasetMapper.map(allowAllQueryContext(), response);

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
