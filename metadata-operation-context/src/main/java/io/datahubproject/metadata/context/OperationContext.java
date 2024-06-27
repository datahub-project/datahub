package io.datahubproject.metadata.context;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

/**
 * These contexts define a read/write context which allows more flexibility when reading and writing
 * to various data stores. This context can be considered per **operation** and allows for
 * supporting database read replicas, mirroring or sharding across multiple databases/elasticsearch
 * instances, and separation of data at the storage level.
 *
 * <p>Different operations might also include different EntityRegistries
 *
 * <p>An integral part of the operation's context is additionally the user's identity and this
 * context encompasses the `Authentication` context.
 */
@Builder(toBuilder = true)
@Getter
public class OperationContext {

  /**
   * This should be the primary entry point when a request is made to Rest.li, OpenAPI, Graphql or
   * other service layers.
   *
   * <p>Copy the context from a system level context to a specific request/user context. Inheriting
   * all other contexts except for the sessionActor. Consider this a down leveling of the access.
   *
   * <p>This allows the context to contain system context such as elasticsearch and database
   * contexts which are inherited from the system.
   *
   * @param systemOperationContext the base operation context
   * @param sessionAuthentication the lower level authentication
   * @param allowSystemAuthentication whether the context is allowed to escalate as needed
   * @return the new context
   */
  @Nonnull
  public static OperationContext asSession(
      OperationContext systemOperationContext,
      @Nonnull RequestContext requestContext,
      @Nonnull Authorizer authorizer,
      @Nonnull Authentication sessionAuthentication,
      boolean allowSystemAuthentication) {
    return systemOperationContext.toBuilder()
        .operationContextConfig(
            // update allowed system authentication
            systemOperationContext.getOperationContextConfig().toBuilder()
                .allowSystemAuthentication(allowSystemAuthentication)
                .build())
        .authorizerContext(AuthorizerContext.builder().authorizer(authorizer).build())
        .requestContext(requestContext)
        // Initialize view authorization for user viewable urn tracking
        .viewAuthorizationContext(ViewAuthorizationContext.builder().build())
        .build(sessionAuthentication);
  }

  /**
   * Apply a set of default flags on top of any existing search flags
   *
   * @param opContext
   * @param flagDefaults
   * @return
   */
  public static OperationContext withSearchFlags(
      OperationContext opContext, Function<SearchFlags, SearchFlags> flagDefaults) {

    return opContext.toBuilder()
        // update search flags for the request's session
        .searchContext(opContext.getSearchContext().withFlagDefaults(flagDefaults))
        .build(opContext.getSessionActorContext());
  }

  /**
   * Apply a set of default flags on top of any existing lineage flags
   *
   * @param opContext
   * @param flagDefaults
   * @return
   */
  public static OperationContext withLineageFlags(
      OperationContext opContext, Function<LineageFlags, LineageFlags> flagDefaults) {

    return opContext.toBuilder()
        // update lineage flags for the request's session
        .searchContext(opContext.getSearchContext().withLineageFlagDefaults(flagDefaults))
        .build(opContext.getSessionActorContext());
  }

  /**
   * Set the system authentication object AND allow escalation of privilege for the session. This
   * OperationContext typically serves the default.
   *
   * <p>If you'd like to set the system authentication but not allow escalation, use the
   * systemActorContext() directly which does not reconfigure the escalation configuration.
   *
   * @param systemAuthentication the system authentication
   * @return builder
   */
  public static OperationContext asSystem(
      @Nonnull OperationContextConfig config,
      @Nonnull Authentication systemAuthentication,
      @Nonnull EntityRegistry entityRegistry,
      @Nullable ServicesRegistryContext servicesRegistryContext,
      @Nullable IndexConvention indexConvention,
      @Nullable RetrieverContext retrieverContext) {
    return asSystem(
        config,
        systemAuthentication,
        entityRegistry,
        servicesRegistryContext,
        indexConvention,
        retrieverContext,
        ObjectMapperContext.DEFAULT);
  }

  public static OperationContext asSystem(
      @Nonnull OperationContextConfig config,
      @Nonnull Authentication systemAuthentication,
      @Nullable EntityRegistry entityRegistry,
      @Nullable ServicesRegistryContext servicesRegistryContext,
      @Nullable IndexConvention indexConvention,
      @Nullable RetrieverContext retrieverContext,
      @Nonnull ObjectMapperContext objectMapperContext) {

    ActorContext systemActorContext =
        ActorContext.builder().systemAuth(true).authentication(systemAuthentication).build();
    OperationContextConfig systemConfig =
        config.toBuilder().allowSystemAuthentication(true).build();
    SearchContext systemSearchContext =
        indexConvention == null
            ? SearchContext.EMPTY
            : SearchContext.builder().indexConvention(indexConvention).build();

    return OperationContext.builder()
        .operationContextConfig(systemConfig)
        .systemActorContext(systemActorContext)
        .searchContext(systemSearchContext)
        .entityRegistryContext(EntityRegistryContext.builder().build(entityRegistry))
        .servicesRegistryContext(servicesRegistryContext)
        // Authorizer.EMPTY doesn't actually apply to system auth
        .authorizerContext(AuthorizerContext.builder().authorizer(Authorizer.EMPTY).build())
        .retrieverContext(retrieverContext)
        .objectMapperContext(objectMapperContext)
        .build(systemAuthentication);
  }

  @Nonnull private final OperationContextConfig operationContextConfig;
  @Nonnull private final ActorContext sessionActorContext;
  @Nullable private final ActorContext systemActorContext;
  @Nonnull private final SearchContext searchContext;
  @Nonnull private final AuthorizerContext authorizerContext;
  @Nonnull private final EntityRegistryContext entityRegistryContext;
  @Nullable private final ServicesRegistryContext servicesRegistryContext;
  @Nullable private final RequestContext requestContext;
  @Nullable private final ViewAuthorizationContext viewAuthorizationContext;
  @Nullable private final RetrieverContext retrieverContext;
  @Nonnull private final ObjectMapperContext objectMapperContext;

  public OperationContext withSearchFlags(
      @Nonnull Function<SearchFlags, SearchFlags> flagDefaults) {
    return OperationContext.withSearchFlags(this, flagDefaults);
  }

  public OperationContext withLineageFlags(
      @Nonnull Function<LineageFlags, LineageFlags> flagDefaults) {
    return OperationContext.withLineageFlags(this, flagDefaults);
  }

  public OperationContext asSession(
      @Nonnull RequestContext requestContext,
      @Nonnull Authorizer authorizer,
      @Nonnull Authentication sessionAuthentication) {
    return OperationContext.asSession(
        this,
        requestContext,
        authorizer,
        sessionAuthentication,
        getOperationContextConfig().isAllowSystemAuthentication());
  }

  @Nonnull
  public EntityRegistry getEntityRegistry() {
    return entityRegistryContext.getEntityRegistry();
  }

  @Nonnull
  public Set<String> getEntityAspectNames(String entityType) {
    return getEntityRegistryContext().getEntityAspectNames(entityType);
  }

  @Nonnull
  public Set<String> getEntityAspectNames(Urn urn) {
    return getEntityRegistryContext().getEntityAspectNames(urn);
  }

  @Nonnull
  public String getKeyAspectName(@Nonnull final Urn urn) {
    return getEntityRegistryContext().getKeyAspectName(urn);
  }

  /**
   * Requests for a generic authentication should return the system first if allowed.
   *
   * @return an entity client
   */
  @Nonnull
  public ActorContext getActorContext() {
    if (operationContextConfig.isAllowSystemAuthentication() && systemActorContext != null) {
      return systemActorContext;
    } else {
      return sessionActorContext;
    }
  }

  /**
   * Other users within the same group as the actor
   *
   * @return
   */
  public Collection<Urn> getActorPeers() {
    return authorizerContext.getAuthorizer().getActorPeers(sessionActorContext.getActorUrn());
  }

  /**
   * Whether default authentication is system level
   *
   * @return
   */
  public boolean isSystemAuth() {
    return operationContextConfig.isAllowSystemAuthentication()
        && sessionActorContext.isSystemAuth();
  }

  /**
   * Requests for a generic authentication should return the system first if allowed.
   *
   * @return an entity client
   */
  public Authentication getAuthentication() {
    return getActorContext().getAuthentication();
  }

  public Authentication getSessionAuthentication() {
    return sessionActorContext.getAuthentication();
  }

  public Optional<Authentication> getSystemAuthentication() {
    return Optional.ofNullable(systemActorContext).map(ActorContext::getAuthentication);
  }

  /** AuditStamp prefer session authentication */
  public AuditStamp getAuditStamp(@Nullable Long currentTimeMs) {
    return AuditStampUtils.getAuditStamp(
        UrnUtils.getUrn(sessionActorContext.getAuthentication().getActor().toUrnStr()),
        currentTimeMs);
  }

  public AuditStamp getAuditStamp() {
    return getAuditStamp(null);
  }

  public Optional<ViewAuthorizationContext> getViewAuthorizationContext() {
    return Optional.ofNullable(viewAuthorizationContext);
  }

  public Optional<RetrieverContext> getRetrieverContext() {
    return Optional.ofNullable(retrieverContext);
  }

  @Nullable
  public AspectRetriever getAspectRetriever() {
    return getAspectRetrieverOpt().orElse(null);
  }

  public Optional<AspectRetriever> getAspectRetrieverOpt() {
    return getRetrieverContext().map(RetrieverContext::getAspectRetriever);
  }

  /**
   * Return a unique id for this context. Typically useful for building cache keys. We combine the
   * different context components to create a single string representation of the hashcode across
   * the contexts.
   *
   * <p>The overall context id can be comprised of one or more other contexts depending on the
   * requirements.
   *
   * @return id representing this context instance's unique identifier
   */
  public String getGlobalContextId() {
    return String.valueOf(
        ImmutableSet.<ContextInterface>builder()
            .add(getOperationContextConfig())
            .add(getAuthorizerContext())
            .add(getSessionActorContext())
            .add(getSearchContext())
            .add(
                getEntityRegistryContext() == null
                    ? EmptyContext.EMPTY
                    : getEntityRegistryContext())
            .add(
                getServicesRegistryContext() == null
                    ? EmptyContext.EMPTY
                    : getServicesRegistryContext())
            .add(getRequestContext() == null ? EmptyContext.EMPTY : getRequestContext())
            .add(
                getViewAuthorizationContext().isPresent()
                    ? getViewAuthorizationContext().get()
                    : EmptyContext.EMPTY)
            .add(
                getRetrieverContext().isPresent()
                    ? getRetrieverContext().get()
                    : EmptyContext.EMPTY)
            .add(getObjectMapperContext())
            .build()
            .stream()
            .map(ContextInterface::getCacheKeyComponent)
            .filter(Optional::isPresent)
            .mapToInt(Optional::get)
            .sum());
  }

  // Context id specific to contexts which impact search responses
  public String getSearchContextId() {
    return String.valueOf(
        ImmutableSet.<ContextInterface>builder()
            .add(getOperationContextConfig())
            .add(getSessionActorContext())
            .add(getSearchContext())
            .add(
                getEntityRegistryContext() == null
                    ? EmptyContext.EMPTY
                    : getEntityRegistryContext())
            .add(
                getServicesRegistryContext() == null
                    ? EmptyContext.EMPTY
                    : getServicesRegistryContext())
            .add(
                getRetrieverContext().isPresent()
                    ? getRetrieverContext().get()
                    : EmptyContext.EMPTY)
            .build()
            .stream()
            .map(ContextInterface::getCacheKeyComponent)
            .filter(Optional::isPresent)
            .mapToInt(Optional::get)
            .sum());
  }

  // Context id specific to entity lookups (not search)
  public String getEntityContextId() {
    return String.valueOf(
        ImmutableSet.<ContextInterface>builder()
            .add(getOperationContextConfig())
            .add(getSessionActorContext())
            .add(
                getEntityRegistryContext() == null
                    ? EmptyContext.EMPTY
                    : getEntityRegistryContext())
            .add(
                getServicesRegistryContext() == null
                    ? EmptyContext.EMPTY
                    : getServicesRegistryContext())
            .build()
            .stream()
            .map(ContextInterface::getCacheKeyComponent)
            .filter(Optional::isPresent)
            .mapToInt(Optional::get)
            .sum());
  }

  @Nonnull
  public String getRequestID() {
    return Optional.ofNullable(requestContext).map(RequestContext::getRequestID).orElse("");
  }

  @Nonnull
  public ObjectMapper getObjectMapper() {
    return objectMapperContext.getObjectMapper();
  }

  public static class OperationContextBuilder {

    @Nonnull
    public OperationContext build(@Nonnull Authentication sessionAuthentication) {
      final Urn actorUrn = UrnUtils.getUrn(sessionAuthentication.getActor().toUrnStr());
      final ActorContext sessionActor =
          ActorContext.builder()
              .authentication(sessionAuthentication)
              .systemAuth(
                  this.systemActorContext != null
                      && this.systemActorContext
                          .getAuthentication()
                          .getActor()
                          .equals(sessionAuthentication.getActor()))
              .policyInfoSet(this.authorizerContext.getAuthorizer().getActorPolicies(actorUrn))
              .groupMembership(this.authorizerContext.getAuthorizer().getActorGroups(actorUrn))
              .build();
      return build(sessionActor);
    }

    @Nonnull
    public OperationContext build(@Nonnull ActorContext sessionActor) {
      return new OperationContext(
          this.operationContextConfig,
          sessionActor,
          this.systemActorContext,
          Objects.requireNonNull(this.searchContext),
          Objects.requireNonNull(this.authorizerContext),
          this.entityRegistryContext,
          this.servicesRegistryContext,
          this.requestContext,
          this.viewAuthorizationContext,
          this.retrieverContext,
          this.objectMapperContext != null
              ? this.objectMapperContext
              : ObjectMapperContext.DEFAULT);
    }

    private OperationContext build() {
      return null;
    }
  }
}
