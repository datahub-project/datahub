package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.ACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.METADATA_POLICY_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.ValidationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test class to reproduce the issue where users with dataset permissions do not have access to
 * schema field resources under that dataset.
 *
 * <p>Issue: When a user has VIEW_ENTITY_PAGE permission on a dataset, they should also be able to
 * access schema fields within that dataset, but currently this fails.
 *
 * <p>Example failing URN:
 * urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:iceberg,test_database.test_schema.test_table,PROD),[version=2.0].[type=struct].[type=string].testField)
 */
public class SchemaFieldPermissionTest {

  public static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:iceberg,test_database.test_schema.test_table,PROD)");
  private static final Urn TEST_SCHEMA_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:iceberg,test_database.test_schema.test_table,PROD),[version=2.0].[type=struct].[type=string].testField)");

  private SystemEntityClient _entityClient;
  private DataHubAuthorizer _dataHubAuthorizer;
  private OperationContext systemOpContext;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = mock(SystemEntityClient.class);

    // Create a policy that grants VIEW_ENTITY_PAGE privilege on datasets to our test user
    final Urn datasetPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:dataset-view-policy");
    final DataHubPolicyInfo datasetViewPolicy = createDatasetViewPolicy(TEST_USER_URN);
    final EnvelopedAspectMap datasetPolicyAspectMap = new EnvelopedAspectMap();
    datasetPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(datasetViewPolicy.data())));

    // Mock the policy search to return our dataset view policy
    final ScrollResult policySearchResult =
        new ScrollResult()
            .setScrollId("1")
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(datasetPolicyUrn))));

    // Mock scrollAcrossEntities to return our policy
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            isNull(),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult);

    // Mock follow-up calls for pagination
    final ScrollResult emptyResult =
        new ScrollResult().setNumEntities(0).setEntities(new SearchEntityArray());

    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("1"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(emptyResult);

    // Mock batchGetV2 to return our policy
    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(POLICY_ENTITY_NAME), any(), anySet()))
        .thenAnswer(
            args -> {
              return Map.of(
                  datasetPolicyUrn,
                  new EntityResponse().setUrn(datasetPolicyUrn).setAspects(datasetPolicyAspectMap));
            });

    // Create system operation context
    final Authentication systemAuthentication =
        new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
    systemOpContext =
        OperationContext.asSystem(
            OperationContextConfig.builder().build(),
            systemAuthentication,
            mock(EntityRegistry.class),
            mock(ServicesRegistryContext.class),
            SearchContext.EMPTY,
            mock(RetrieverContext.class),
            mock(ValidationContext.class),
            null,
            true);

    // Initialize the DataHub authorizer
    _dataHubAuthorizer =
        new DataHubAuthorizer(
            systemOpContext, _entityClient, 10, 10, DataHubAuthorizer.AuthorizationMode.DEFAULT, 1);

    _dataHubAuthorizer.init(
        Collections.emptyMap(), createAuthorizerContext(systemOpContext, _entityClient));
    _dataHubAuthorizer.invalidateCache();
    Thread.sleep(500); // Allow cache refresh
  }

  @Test
  public void testDatasetPermissionGrantsAccess() throws Exception {
    // Test that the user can access the dataset itself
    EntitySpec datasetSpec = new EntitySpec("dataset", TEST_DATASET_URN.toString());

    AuthorizationRequest datasetRequest =
        new AuthorizationRequest(
            TEST_USER_URN.toString(),
            "VIEW_ENTITY_PAGE",
            Optional.of(datasetSpec),
            Collections.emptyList());

    AuthorizationResult datasetResult = _dataHubAuthorizer.authorize(datasetRequest);
    assertEquals(
        datasetResult.getType(),
        AuthorizationResult.Type.ALLOW,
        "User should have VIEW_ENTITY_PAGE access to the dataset");
  }

  @Test
  public void testSchemaFieldPermissionNowWorks() throws Exception {
    // Test that the user CAN now access schema fields under the dataset
    // This test verifies the fix is working correctly
    EntitySpec schemaFieldSpec = new EntitySpec("schemaField", TEST_SCHEMA_FIELD_URN.toString());

    AuthorizationRequest schemaFieldRequest =
        new AuthorizationRequest(
            TEST_USER_URN.toString(),
            "VIEW_ENTITY_PAGE",
            Optional.of(schemaFieldSpec),
            Collections.emptyList());

    AuthorizationResult schemaFieldResult = _dataHubAuthorizer.authorize(schemaFieldRequest);

    // With the fix, schema field access should now be allowed through inheritance
    assertEquals(
        schemaFieldResult.getType(),
        AuthorizationResult.Type.ALLOW,
        "User with dataset permissions should now have access to schema fields under that dataset (fix verified)");

    System.out.println(
        "SUCCESS: Schema field access now works through dataset permission inheritance!");
    System.out.println("Schema Field URN: " + TEST_SCHEMA_FIELD_URN);
    System.out.println(
        "Result: " + schemaFieldResult.getType() + " - " + schemaFieldResult.getMessage());
  }

  @Test
  public void testSchemaFieldPermissionInheritsFromDataset() throws Exception {
    // This test verifies that schema field permissions properly inherit from dataset permissions

    // First verify dataset access works
    EntitySpec datasetSpec = new EntitySpec("dataset", TEST_DATASET_URN.toString());
    AuthorizationRequest datasetRequest =
        new AuthorizationRequest(
            TEST_USER_URN.toString(),
            "VIEW_ENTITY_PAGE",
            Optional.of(datasetSpec),
            Collections.emptyList());

    AuthorizationResult datasetResult = _dataHubAuthorizer.authorize(datasetRequest);
    assertEquals(
        datasetResult.getType(),
        AuthorizationResult.Type.ALLOW,
        "Dataset access should be allowed");

    // Now test schema field access - this should inherit from dataset
    EntitySpec schemaFieldSpec = new EntitySpec("schemaField", TEST_SCHEMA_FIELD_URN.toString());
    AuthorizationRequest schemaFieldRequest =
        new AuthorizationRequest(
            TEST_USER_URN.toString(),
            "VIEW_ENTITY_PAGE",
            Optional.of(schemaFieldSpec),
            Collections.emptyList());

    AuthorizationResult schemaFieldResult = _dataHubAuthorizer.authorize(schemaFieldRequest);

    // After the fix, this should now work
    assertEquals(
        schemaFieldResult.getType(),
        AuthorizationResult.Type.ALLOW,
        "Schema field access should inherit from parent dataset permissions");

    System.out.println("SUCCESS: Schema field permission inheritance working correctly");
    System.out.println("Dataset URN: " + TEST_DATASET_URN);
    System.out.println("Schema Field URN: " + TEST_SCHEMA_FIELD_URN);
    System.out.println(
        "Result: " + schemaFieldResult.getType() + " - " + schemaFieldResult.getMessage());
  }

  /** Creates a policy that grants VIEW_ENTITY_PAGE privilege on datasets to a specific user */
  private DataHubPolicyInfo createDatasetViewPolicy(Urn userUrn) throws Exception {
    final DataHubPolicyInfo policyInfo = new DataHubPolicyInfo();
    policyInfo.setType(METADATA_POLICY_TYPE);
    policyInfo.setState(ACTIVE_POLICY_STATE);
    policyInfo.setPrivileges(new StringArray(ImmutableList.of("VIEW_ENTITY_PAGE")));
    policyInfo.setDisplayName("Dataset View Policy");
    policyInfo.setDescription("Policy to grant dataset view access to test user");
    policyInfo.setEditable(true);

    // Set the actor (user who gets the permission)
    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setUsers(new UrnArray(ImmutableList.of(userUrn)));
    policyInfo.setActors(actorFilter);

    // Set the resource filter (applies to all datasets)
    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setType("dataset");
    resourceFilter.setAllResources(true);
    policyInfo.setResources(resourceFilter);

    return policyInfo;
  }

  private AuthorizerContext createAuthorizerContext(
      final OperationContext systemOpContext, final SystemEntityClient entityClient) {
    return new AuthorizerContext(
        Collections.emptyMap(), new DefaultEntitySpecResolver(systemOpContext, entityClient));
  }
}
