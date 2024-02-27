package io.datahubproject.test.metadata.context;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.config.SearchAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
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
  public static final IndexConvention TEST_EMPTY_INDEX_CONVENTION = IndexConventionImpl.NO_PREFIX;

  public static OperationContext systemContextNoSearchAuthorization(
      @Nonnull EntityRegistry entityRegistry) {
    return systemContextNoSearchAuthorization(entityRegistry, null);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nonnull EntityRegistry entityRegistry, @Nullable IndexConvention indexConvention) {
    return OperationContext.asSystem(
        OperationContextConfig.builder()
            .searchAuthorizationConfiguration(
                SearchAuthorizationConfiguration.builder().enabled(false).build())
            .build(),
        entityRegistry,
        TEST_SYSTEM_AUTH,
        indexConvention != null ? indexConvention : TEST_EMPTY_INDEX_CONVENTION);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Urn userUrn) {
    return userContextNoSearchAuthorization(entityRegistry, Authorizer.EMPTY, userUrn);
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Authorizer authorizer,
      @Nonnull Urn userUrn) {
    return userContextNoSearchAuthorization(
        entityRegistry,
        authorizer,
        new Authentication(new Actor(ActorType.USER, userUrn.getId()), ""));
  }

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Authorizer authorizer,
      @Nonnull Authentication sessionAuthorization) {
    return systemContextNoSearchAuthorization(entityRegistry)
        .asSession(authorizer, sessionAuthorization);
  }

  private TestOperationContexts() {}
}
