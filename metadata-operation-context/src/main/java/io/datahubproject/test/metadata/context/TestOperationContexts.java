package io.datahubproject.test.metadata.context;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;

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

  public static AspectRetriever emptyAspectRetriever(
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier) {
    return new EmptyAspectRetriever(
        () ->
            Optional.ofNullable(entityRegistrySupplier)
                .map(Supplier::get)
                .orElse(defaultEntityRegistry()));
  }

  public static GraphRetriever emptyGraphRetriever = new EmptyGraphRetriever();
  public static SearchRetriever emptySearchRetriever = new EmptySearchRetriever();

  public static RetrieverContext emptyRetrieverContext(
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier) {

    return RetrieverContext.builder()
        .aspectRetriever(emptyAspectRetriever(entityRegistrySupplier))
        .graphRetriever(emptyGraphRetriever)
        .searchRetriever(emptySearchRetriever)
        .build();
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
            .graphRetriever(emptyGraphRetriever)
            .searchRetriever(emptySearchRetriever)
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
        null);
  }

  public static OperationContext systemContext(
      @Nullable Supplier<OperationContextConfig> configSupplier,
      @Nullable Supplier<Authentication> systemAuthSupplier,
      @Nullable Supplier<ServicesRegistryContext> servicesRegistrySupplier,
      @Nullable Supplier<EntityRegistry> entityRegistrySupplier,
      @Nullable Supplier<RetrieverContext> retrieverContextSupplier,
      @Nullable Supplier<IndexConvention> indexConventionSupplier,
      @Nullable Consumer<OperationContext> postConstruct) {

    OperationContextConfig config =
        Optional.ofNullable(configSupplier).map(Supplier::get).orElse(DEFAULT_OPCONTEXT_CONFIG);

    Authentication systemAuth =
        Optional.ofNullable(systemAuthSupplier).map(Supplier::get).orElse(TEST_SYSTEM_AUTH);

    RetrieverContext retrieverContext =
        Optional.ofNullable(retrieverContextSupplier)
            .map(Supplier::get)
            .orElse(emptyRetrieverContext(entityRegistrySupplier));

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

    OperationContext operationContext =
        OperationContext.asSystem(
            config,
            systemAuth,
            entityRegistry,
            servicesRegistryContext,
            indexConvention,
            retrieverContext);

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

  @Builder
  public static class EmptyAspectRetriever implements AspectRetriever {
    private final Supplier<EntityRegistry> entityRegistrySupplier;

    @Nonnull
    @Override
    public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
        Set<Urn> urns, Set<String> aspectNames) {
      return Map.of();
    }

    @Nonnull
    @Override
    public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
        Map<Urn, Set<String>> urnAspectNames) {
      return Map.of();
    }

    @Nonnull
    @Override
    public EntityRegistry getEntityRegistry() {
      return entityRegistrySupplier.get();
    }
  }

  public static class EmptyGraphRetriever implements GraphRetriever {

    @Nonnull
    @Override
    public RelatedEntitiesScrollResult scrollRelatedEntities(
        @Nullable List<String> sourceTypes,
        @Nonnull Filter sourceEntityFilter,
        @Nullable List<String> destinationTypes,
        @Nonnull Filter destinationEntityFilter,
        @Nonnull List<String> relationshipTypes,
        @Nonnull RelationshipFilter relationshipFilter,
        @Nonnull List<SortCriterion> sortCriterion,
        @Nullable String scrollId,
        int count,
        @Nullable Long startTimeMillis,
        @Nullable Long endTimeMillis) {
      return new RelatedEntitiesScrollResult(0, 0, null, List.of());
    }
  }

  public static class EmptySearchRetriever implements SearchRetriever {

    @Override
    public ScrollResult scroll(
        @Nonnull List<String> entities,
        @Nullable Filter filters,
        @Nullable String scrollId,
        int count) {
      ScrollResult empty = new ScrollResult();
      empty.setEntities(new SearchEntityArray());
      empty.setNumEntities(0);
      empty.setPageSize(0);
      return empty;
    }
  }

  private TestOperationContexts() {}
}
