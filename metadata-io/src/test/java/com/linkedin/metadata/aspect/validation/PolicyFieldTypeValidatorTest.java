package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PolicyFieldTypeValidatorTest {
  private static final Urn TEST_POLICY_URN = UrnUtils.getUrn("urn:li:dataHubPolicy:test-policy");

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(PolicyFieldTypeValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE", "PATCH"))
          .supportedEntityAspectNames(
              List.of(
                  new AspectPluginConfig.EntityAspectName(
                      POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME)))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;

  private PolicyFieldTypeValidator validator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new PolicyFieldTypeValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testValidFieldTypeInFilter() {
    DataHubPolicyInfo policyInfo = createPolicyInfoWithFilter("TYPE", "dataset");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for valid field type TYPE");
  }

  @Test
  public void testValidFieldTypeInPrivilegeConstraints() {
    DataHubPolicyInfo policyInfo = createPolicyInfoWithPrivilegeConstraints("DOMAIN");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for valid field type DOMAIN");
  }

  @Test
  public void testInvalidFieldTypeInFilter() {
    DataHubPolicyInfo policyInfo = createPolicyInfoWithFilter("INVALID_FIELD_TYPE", "somevalue");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected validation to fail for invalid field type INVALID_FIELD_TYPE");
  }

  @Test
  public void testInvalidFieldTypeInPrivilegeConstraints() {
    DataHubPolicyInfo policyInfo = createPolicyInfoWithPrivilegeConstraints("INVALID_CONSTRAINT");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected validation to fail for invalid field type INVALID_CONSTRAINT");
  }

  @Test
  public void testMultipleValidFieldTypes() {
    DataHubPolicyInfo policyInfo = createPolicyInfoWithMultipleFilters("TYPE", "DOMAIN");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for multiple valid field types");
  }

  @Test
  public void testMixedValidAndInvalidFieldTypes() {
    DataHubPolicyInfo policyInfo = createPolicyInfoWithMultipleFilters("TYPE", "INVALID_FIELD");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected validation to fail when at least one field type is invalid");
  }

  @Test
  public void testDeprecatedFieldTypesStillValid() {
    DataHubPolicyInfo policyInfo =
        createPolicyInfoWithFilter("RESOURCE_URN", "urn:li:dataset:test");

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for deprecated but still valid field type RESOURCE_URN");
  }

  @Test
  public void testPolicyWithoutResources() {
    DataHubPolicyInfo policyInfo = createBasicPolicyInfo();

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for policy without resources");
  }

  @Test
  public void testPolicyWithoutFilter() {
    DataHubPolicyInfo policyInfo = createBasicPolicyInfo();
    DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    policyInfo.setResources(resourceFilter);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for policy without filter");
  }

  private DataHubPolicyInfo createBasicPolicyInfo() {
    return new DataHubPolicyInfo()
        .setActors(new DataHubActorFilter())
        .setEditable(true)
        .setDescription("Test Policy")
        .setDisplayName("Test Policy")
        .setLastUpdatedTimestamp(123L)
        .setPrivileges(new StringArray("EDIT_ENTITY"))
        .setState("ACTIVE")
        .setType("METADATA");
  }

  private DataHubPolicyInfo createPolicyInfoWithFilter(String fieldType, String value) {
    DataHubPolicyInfo policyInfo = createBasicPolicyInfo();

    PolicyMatchCriterion criterion = new PolicyMatchCriterion();
    criterion.setField(fieldType);
    criterion.setValues(new StringArray(value));
    criterion.setCondition(PolicyMatchCondition.EQUALS);

    PolicyMatchFilter filter = new PolicyMatchFilter();
    filter.setCriteria(new PolicyMatchCriterionArray(criterion));

    DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setFilter(filter);

    policyInfo.setResources(resourceFilter);
    return policyInfo;
  }

  private DataHubPolicyInfo createPolicyInfoWithPrivilegeConstraints(String fieldType) {
    DataHubPolicyInfo policyInfo = createBasicPolicyInfo();

    PolicyMatchCriterion criterion = new PolicyMatchCriterion();
    criterion.setField(fieldType);
    criterion.setValues(new StringArray("urn:li:domain:engineering"));
    criterion.setCondition(PolicyMatchCondition.EQUALS);

    PolicyMatchFilter filter = new PolicyMatchFilter();
    filter.setCriteria(new PolicyMatchCriterionArray(criterion));

    DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setPrivilegeConstraints(filter);

    policyInfo.setResources(resourceFilter);
    return policyInfo;
  }

  private DataHubPolicyInfo createPolicyInfoWithMultipleFilters(
      String fieldType1, String fieldType2) {
    DataHubPolicyInfo policyInfo = createBasicPolicyInfo();

    PolicyMatchCriterion criterion1 = new PolicyMatchCriterion();
    criterion1.setField(fieldType1);
    criterion1.setValues(new StringArray("value1"));
    criterion1.setCondition(PolicyMatchCondition.EQUALS);

    PolicyMatchCriterion criterion2 = new PolicyMatchCriterion();
    criterion2.setField(fieldType2);
    criterion2.setValues(new StringArray("value2"));
    criterion2.setCondition(PolicyMatchCondition.EQUALS);

    PolicyMatchFilter filter = new PolicyMatchFilter();
    filter.setCriteria(new PolicyMatchCriterionArray(criterion1, criterion2));

    DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setFilter(filter);

    policyInfo.setResources(resourceFilter);
    return policyInfo;
  }
}
