package com.linkedin.gms;

import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FilterUtils;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Production {@code application.yaml} with one policy served by the entity client. Exercises the
 * {@code authorizerChain} bean end to end (chain, default authorizer, resolver, policy engine): a
 * view policy scoped to {@code container} and {@code schemaField} must grant on both types and on
 * nothing else.
 */
@ContextConfiguration(classes = SpringAuthorizerChainTest.PolicySource.class)
public class SpringAuthorizerChainTest extends AbstractGmsSpringContextTest {

  private static final Urn POLICY_URN = UrnUtils.getUrn("urn:li:dataHubPolicy:view-test");
  private static final String VIEWER = "urn:li:corpuser:viewer";
  private static final String OTHER_USER = "urn:li:corpuser:other";
  private static final String VIEW_PRIVILEGE = PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE.getType();

  private static final DataHubPolicyInfo VIEW_POLICY = viewPolicy();

  private static final EntitySpec CONTAINER =
      new EntitySpec(CONTAINER_ENTITY_NAME, "urn:li:container:view-test");
  private static final EntitySpec SCHEMA_FIELD =
      new EntitySpec(
          SCHEMA_FIELD_ENTITY_NAME,
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD),col_a)");
  private static final EntitySpec DATASET =
      new EntitySpec(
          DATASET_ENTITY_NAME, "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");

  @Autowired
  @Qualifier("authorizerChain")
  private AuthorizerChain authorizerChain;

  @Autowired
  @Qualifier("dataHubAuthorizer")
  private DataHubAuthorizer dataHubAuthorizer;

  @BeforeClass
  public void loadPolicy() throws Exception {
    dataHubAuthorizer.invalidateCache();
    awaitPolicyLoaded();
  }

  @Test
  public void testViewPolicyAppliesToContainerAndSchemaField() {
    assertEquals(authorize(VIEWER, CONTAINER), AuthorizationResult.Type.ALLOW);
    assertEquals(authorize(VIEWER, SCHEMA_FIELD), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testViewPolicyDoesNotLeakBeyondItsScope() {
    assertEquals(authorize(VIEWER, DATASET), AuthorizationResult.Type.DENY);
    assertEquals(authorize(OTHER_USER, CONTAINER), AuthorizationResult.Type.DENY);
    assertEquals(authorize(OTHER_USER, SCHEMA_FIELD), AuthorizationResult.Type.DENY);
  }

  private AuthorizationResult.Type authorize(String actor, EntitySpec resource) {
    return authorizerChain
        .authorize(
            new AuthorizationRequest(actor, VIEW_PRIVILEGE, Optional.of(resource), List.of()))
        .getType();
  }

  /** The policy cache refreshes on the authorizer's executor; wait for it to see the policy. */
  private void awaitPolicyLoaded() throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    // getActorPolicies always includes built-in defaults (e.g. view self), so look for ours.
    while (!dataHubAuthorizer.getActorPolicies(UrnUtils.getUrn(VIEWER)).contains(VIEW_POLICY)) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("policy cache never loaded the test policy");
      }
      Thread.sleep(100);
    }
  }

  private static DataHubPolicyInfo viewPolicy() {
    return new DataHubPolicyInfo()
        .setType(PoliciesConfig.METADATA_POLICY_TYPE)
        .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
        .setDisplayName("View containers and fields")
        .setEditable(true)
        .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
        .setActors(new DataHubActorFilter().setUsers(new UrnArray(UrnUtils.getUrn(VIEWER))))
        .setResources(
            new DataHubResourceFilter()
                .setFilter(
                    FilterUtils.newFilter(
                        Map.of(
                            EntityFieldType.TYPE,
                            List.of(CONTAINER_ENTITY_NAME, SCHEMA_FIELD_ENTITY_NAME)))));
  }

  /**
   * Wraps the production {@code systemEntityClient} in a spy that serves the test policy. Stubbed
   * here, before {@link DataHubAuthorizer} exists, so no refresh thread can call the spy while it
   * is being stubbed; everything but the policy fetch stays on the production client.
   */
  @TestConfiguration
  static class PolicySource {
    @Bean
    static BeanPostProcessor servePolicyFromSystemEntityClient() {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
          return "systemEntityClient".equals(beanName)
              ? servingPolicy((SystemEntityClient) bean)
              : bean;
        }
      };
    }

    private static SystemEntityClient servingPolicy(SystemEntityClient client) {
      SystemEntityClient spy = Mockito.spy(client);
      try {
        doReturn(
                new ScrollResult()
                    .setNumEntities(1)
                    .setEntities(
                        new SearchEntityArray(List.of(new SearchEntity().setEntity(POLICY_URN)))))
            .when(spy)
            .scrollAcrossEntities(
                any(OperationContext.class),
                eq(List.of(POLICY_ENTITY_NAME)),
                anyString(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                anyList(),
                anyInt());

        EnvelopedAspectMap aspects = new EnvelopedAspectMap();
        aspects.put(
            DATAHUB_POLICY_INFO_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(VIEW_POLICY.data())));
        doReturn(Map.of(POLICY_URN, new EntityResponse().setUrn(POLICY_URN).setAspects(aspects)))
            .when(spy)
            .batchGetV2(any(OperationContext.class), eq(POLICY_ENTITY_NAME), anySet(), anySet());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return spy;
    }
  }
}
