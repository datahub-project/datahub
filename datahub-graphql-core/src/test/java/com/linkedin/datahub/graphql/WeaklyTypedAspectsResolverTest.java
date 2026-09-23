package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.linkedin.datahub.graphql.generated.AspectParams;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RawAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class WeaklyTypedAspectsResolverTest {

  @Test
  public void testCredentialsAspectOmittedWithoutManageUserCredentials() throws Exception {
    EntityClient client = mock(EntityClient.class);
    EntityRegistry registry = mock(EntityRegistry.class);
    EntitySpec entitySpec = mock(EntitySpec.class);
    AspectSpec credentialsSpec = mock(AspectSpec.class);
    when(credentialsSpec.getName()).thenReturn("corpUserCredentials");
    when(credentialsSpec.isAutoRender()).thenReturn(false);
    when(entitySpec.getAspectSpecs()).thenReturn(List.of(credentialsSpec));
    when(registry.getEntitySpec("corpuser")).thenReturn(entitySpec);

    Entity source = mock(Entity.class);
    when(source.getUrn()).thenReturn("urn:li:corpuser:victim");
    when(source.getType()).thenReturn(EntityType.CORP_USER);

    AspectParams params = new AspectParams();
    params.setAspectNames(List.of("corpUserCredentials"));

    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(getMockAllowContext());
    when(env.getSource()).thenReturn(source);
    when(env.getArgument("input")).thenReturn(params);

    WeaklyTypedAspectsResolver resolver = new WeaklyTypedAspectsResolver(client, registry);

    try (MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {
      authUtil
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      any(AuthorizationSession.class),
                      eq(PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE)))
          .thenReturn(false);

      List<RawAspect> result = resolver.get(env).join();
      assertTrue(result.isEmpty());
      verify(client, never()).batchGetV2(any(), any(), any(), any());
    }
  }
}
