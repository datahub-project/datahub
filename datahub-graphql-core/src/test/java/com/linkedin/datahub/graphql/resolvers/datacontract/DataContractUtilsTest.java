package com.linkedin.datahub.graphql.resolvers.datacontract;

import static org.mockito.Mockito.mock;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.Assert;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Optional;
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
                Mockito.when(authorizer.authorize(Mockito.any(AuthorizationRequest.class)))
                    .thenReturn(
                        new AuthorizationResult(
                            new AuthorizationRequest(
                                "TEST", "test", Optional.of(new EntitySpec("dataset", "test"))),
                            AuthorizationResult.Type.ALLOW,
                            "TEST"));
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
