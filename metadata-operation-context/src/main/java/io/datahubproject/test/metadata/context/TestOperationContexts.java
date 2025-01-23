package io.datahubproject.test.metadata.context;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.ValidationContext;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

  private static final OperationContextConfig DEFAULT_OPCONTEXT_CONFIG =
      OperationContextConfig.builder()
          .viewAuthorizationConfiguration(
              ViewAuthorizationConfiguration.builder().enabled(false).build())
          .build();

  private static EntityRegistry defaultEntityRegistryInstance;
  private static ValidationContext defaultValidationContext =
      ValidationContext.builder().alternateValidation(false).build();

  public static EntityRegistry defaultEntityRegistry() {
    if (defaultEntityRegistryInstance == null) {
      PathSpecBasedSchemaAnnotationVisitor.class
          .getClassLoader()
          .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
      try {
        SnapshotEntityRegistry snapshotEntityRegistry = new SnapshotEntityRegistry();
        ConfigEntityRegistry configEntityRegistry =
            new ConfigEntityRegistry(
                Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
        defaultEntityRegistryInstance =
            new MergedEntityRegistry(snapshotEntityRegistry).apply(configEntityRegistry);
      } catch (EntityRegistryException e) {
        throw new RuntimeException(e);
      }
    }
    return defaultEntityRegistryInstance;
  }

  public static RetrieverContext emptyActiveUsersRetrieverContext(
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier) {

    return RetrieverContext.builder()
        .cachingAspectRetriever(emptyActiveUsersAspectRetriever(entityRegistrySupplier))
        .graphRetriever(GraphRetriever.EMPTY)
        .searchRetriever(SearchRetriever.EMPTY)
        .build();
  }

  public static CachingAspectRetriever emptyActiveUsersAspectRetriever(
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier) {

    return new CachingAspectRetriever.EmptyAspectRetriever() {

      @Nonnull
      @Override
      public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
          Set<Urn> urns, Set<String> aspectNames) {
        if (urns.stream().allMatch(urn -> urn.toString().startsWith("urn:li:corpuser:"))
            && aspectNames.contains(Constants.CORP_USER_KEY_ASPECT_NAME)) {
          return urns.stream()
              .map(
                  urn ->
                      Map.entry(
                          urn,
                          Map.of(
                              Constants.CORP_USER_KEY_ASPECT_NAME,
                              new Aspect(
                                  new CorpUserInfo()
                                      .setActive(true)
                                      .setEmail(urn.getId())
                                      .setDisplayName(urn.getId())
                                      .data()))))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return super.getLatestAspectObjects(urns, aspectNames);
      }

      @Nonnull
      @Override
      public EntityRegistry getEntityRegistry() {
        return Optional.ofNullable(entityRegistrySupplier)
            .map(Supplier::get)
            .orElse(defaultEntityRegistry());
      }
    };
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable EntityRegistry entityRegistry) {
    return systemContextNoSearchAuthorization(entityRegistry, (IndexConvention) null);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable IndexConvention indexConvention) {
    return systemContextNoSearchAuthorization((EntityRegistry) null, indexConvention);
  }

  public static OperationContext systemContextNoSearchAuthorization() {
    return systemContextNoSearchAuthorization(null, null, null);
  }

  public static OperationContext systemContextNoValidate() {
    return systemContextNoSearchAuthorization(
        null, null, null, () -> ValidationContext.builder().alternateValidation(true).build());
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable EntityRegistry entityRegistry, @Nullable IndexConvention indexConvention) {
    return systemContextNoSearchAuthorization(() -> entityRegistry, null, () -> indexConvention);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable RetrieverContext retrieverContext) {
    return systemContextNoSearchAuthorization(
        () -> retrieverContext.getAspectRetriever().getEntityRegistry(),
        () -> retrieverContext,
        null);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable AspectRetriever aspectRetriever) {
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(
                emptyActiveUsersAspectRetriever(() -> aspectRetriever.getEntityRegistry()))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();
    return systemContextNoSearchAuthorization(
        () -> retrieverContext.getAspectRetriever().getEntityRegistry(),
        () -> retrieverContext,
        null);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable Supplier<RetrieverContext> retrieverContextSupplier,
      @Nullable IndexConvention indexConvention) {
    return systemContextNoSearchAuthorization(
        null, retrieverContextSupplier, () -> indexConvention);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier,
      @Nullable Supplier<RetrieverContext> retrieverContextSupplier,
      @Nullable Supplier<IndexConvention> indexConventionSupplier) {

    return systemContext(
        null,
        null,
        null,
        entityRegistrySupplier,
        retrieverContextSupplier,
        indexConventionSupplier,
        null,
        null);
  }

  public static OperationContext systemContextNoSearchAuthorization(
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier,
      @Nullable Supplier<RetrieverContext> retrieverContextSupplier,
      @Nullable Supplier<IndexConvention> indexConventionSupplier,
      @Nullable Supplier<ValidationContext> environmentContextSupplier) {

    return systemContext(
        null,
        null,
        null,
        entityRegistrySupplier,
        retrieverContextSupplier,
        indexConventionSupplier,
        null,
        environmentContextSupplier);
  }

  public static OperationContext systemContext(
      @Nullable Supplier<OperationContextConfig> configSupplier,
      @Nullable Supplier<Authentication> systemAuthSupplier,
      @Nullable Supplier<ServicesRegistryContext> servicesRegistrySupplier,
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier,
      @Nullable Supplier<RetrieverContext> retrieverContextSupplier,
      @Nullable Supplier<IndexConvention> indexConventionSupplier,
      @Nullable Consumer<OperationContext> postConstruct,
      @Nullable Supplier<ValidationContext> environmentContextSupplier) {

    OperationContextConfig config =
        Optional.ofNullable(configSupplier).map(Supplier::get).orElse(DEFAULT_OPCONTEXT_CONFIG);

    Authentication systemAuth =
        Optional.ofNullable(systemAuthSupplier).map(Supplier::get).orElse(TEST_SYSTEM_AUTH);

    RetrieverContext retrieverContext =
        Optional.ofNullable(retrieverContextSupplier)
            .map(Supplier::get)
            .orElse(emptyActiveUsersRetrieverContext(entityRegistrySupplier));

    EntityRegistry entityRegistry =
        Optional.ofNullable(entityRegistrySupplier)
            .map(Supplier::get)
            .orElse(defaultEntityRegistry());

    IndexConvention indexConvention =
        Optional.ofNullable(indexConventionSupplier)
            .map(Supplier::get)
            .orElse(IndexConventionImpl.noPrefix("MD5"));

    ServicesRegistryContext servicesRegistryContext =
        Optional.ofNullable(servicesRegistrySupplier).orElse(() -> null).get();

    ValidationContext validationContext =
        Optional.ofNullable(environmentContextSupplier)
            .map(Supplier::get)
            .orElse(defaultValidationContext);

    OperationContext operationContext =
        OperationContext.asSystem(
            config,
            systemAuth,
            entityRegistry,
            servicesRegistryContext,
            indexConvention,
            retrieverContext,
            validationContext,
            true);

    if (postConstruct != null) {
      postConstruct.accept(operationContext);
    }

    return operationContext;
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
      @Nonnull Authentication sessionAuthorization) {
    return userContextNoSearchAuthorization(Authorizer.EMPTY, sessionAuthorization, null);
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

  public static OperationContext userContextNoSearchAuthorization(
      @Nonnull RequestContext requestContext) {
    return systemContextNoSearchAuthorization(defaultEntityRegistry())
        .asSession(requestContext, Authorizer.EMPTY, TEST_USER_AUTH);
  }

  private TestOperationContexts() {}
}
