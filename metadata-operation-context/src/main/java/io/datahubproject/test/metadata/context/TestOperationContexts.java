package io.datahubproject.test.metadata.context;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Useful for testing. If the defaults are not sufficient, try using the .toBuilder() and replacing
 * the parts that you are interested in customizing.
 */
public class TestOperationContexts {
  public static final Authentication TEST_SYSTEM_AUTH =
      new Authentication(new Actor(ActorType.USER, "testSystemUser"), "");
  public static final Authentication TEST_USER_AUTH =
      new Authentication(new Actor(ActorType.USER, "datahub"), "");

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable EntityRegistry entityRegistry) {
    return systemContextNoSearchAuthorization(entityRegistry, null);
  }

  public static OperationContext systemContextNoSearchAuthorization() {
    return systemContextNoSearchAuthorization(null, null);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable EntityRegistry entityRegistry, @Nullable IndexConvention indexConvention) {
    return OperationContext.asSystem(
        OperationContextConfig.builder()
            .viewAuthorizationConfiguration(
                ViewAuthorizationConfiguration.builder().enabled(false).build())
            .build(),
        TEST_SYSTEM_AUTH,
        entityRegistry,
        null,
        indexConvention);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nullable EntityRegistry entityRegistry) {
    return userContextNoSearchAuthorization(Authorizer.EMPTY, TEST_USER_AUTH, entityRegistry);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull Authorizer authorizer, @Nonnull Urn userUrn) {
    return userContextNoSearchAuthorization(authorizer, userUrn, null);
  }

  public static OperationContext userContextNoSearchAuthorization(@Nonnull Urn userUrn) {
    return userContextNoSearchAuthorization(Authorizer.EMPTY, userUrn, null);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull Urn userUrn, @Nullable EntityRegistry entityRegistry) {
    return userContextNoSearchAuthorization(Authorizer.EMPTY, userUrn, entityRegistry);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull Authorizer authorizer,
      @Nonnull Urn userUrn,
      @Nullable EntityRegistry entityRegistry) {
    return userContextNoSearchAuthorization(
        authorizer,
        new Authentication(new Actor(ActorType.USER, userUrn.getId()), ""),
        entityRegistry);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull Authorizer authorizer, @Nonnull Authentication sessionAuthorization) {
    return userContextNoSearchAuthorization(authorizer, sessionAuthorization, null);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull Authorizer authorizer,
      @Nonnull Authentication sessionAuthorization,
      @Nullable EntityRegistry entityRegistry) {
    return systemContextNoSearchAuthorization(entityRegistry)
        .asSession(RequestContext.TEST, authorizer, sessionAuthorization);
  }

  private TestOperationContexts() {}
}
