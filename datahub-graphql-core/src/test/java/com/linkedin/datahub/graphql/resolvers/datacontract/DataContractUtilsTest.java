package com.linkedin.datahub.graphql.resolvers.datacontract;

import static org.mockito.Mockito.mock;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.BatchAuthorizationRequest;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.test.authorization.ConstantAuthorizationResultMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.Assert;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataContractUtilsTest {

  @Test
  public void testCanEditDataContract() {
    Urn testUrn = UrnUtils.getUrn("urn:li:dataContract:test");
    boolean result =
        DataContractUtils.canEditDataContract(
            new QueryContext() {
              @Override
              public boolean isAuthenticated() {
                return true;
              }

              @Override
              public Authentication getAuthentication() {
                Authentication auth = new Authentication(new Actor(ActorType.USER, "test"), "TEST");
                return auth;
              }

              @Override
              public Authorizer getAuthorizer() {
                Authorizer authorizer = mock(Authorizer.class);
                Mockito.when(
                        authorizer.authorizeBatch(Mockito.any(BatchAuthorizationRequest.class)))
                    .thenReturn(
                        new BatchAuthorizationResult(
                            new BatchAuthorizationRequest(
                                "TEST",
                                Set.of("test"),
                                Optional.of(new EntitySpec("dataset", "test")),
                                Collections.emptyList()),
                            new ConstantAuthorizationResultMap(AuthorizationResult.Type.ALLOW)));
                return authorizer;
              }

              @Override
              public OperationContext getOperationContext() {
                return TestOperationContexts.userContextNoSearchAuthorization(
                    getAuthorizer(), getAuthentication());
              }

              @Override
              public DataHubAppConfiguration getDataHubAppConfig() {
                return new DataHubAppConfiguration();
              }
            },
            testUrn);
    Assert.assertTrue(result);
  }
}
