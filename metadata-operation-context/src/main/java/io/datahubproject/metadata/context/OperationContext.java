package io.datahubproject.metadata.context;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.exception.ActorAccessException;
import io.datahubproject.metadata.exception.OperationContextException;
import io.datahubproject.metadata.exception.TraceException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class OperationContext implements AuthorizationSession {

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
    return OperationContext.asSession(
        systemOperationContext,
        requestContext,
        authorizer,
        sessionAuthentication,
        allowSystemAuthentication,
        false);
  }

  @Nonnull
  public static OperationContext asSession(
      OperationContext systemOperationContext,
      @Nonnull RequestContext requestContext,
      @Nonnull Authorizer authorizer,
      @Nonnull Authentication sessionAuthentication,
      boolean allowSystemAuthentication,
      boolean skipCache)
      throws ActorAccessException {
    return systemOperationContext.toBuilder()
        .operationContextConfig(
            // update allowed system authentication
            systemOperationContext.getOperationContextConfig().toBuilder()
                .allowSystemAuthentication(allowSystemAuthentication)
                .build())
        .authorizationContext(AuthorizationContext.builder().authorizer(authorizer).build())
        .requestContext(requestContext)
        .validationContext(systemOperationContext.getValidationContext())
        .build(sessionAuthentication, skipCache);
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

    try {
      return opContext.toBuilder()
          // update search flags for the request's session
          .searchContext(opContext.getSearchContext().withFlagDefaults(flagDefaults))
          .build(opContext.getSessionActorContext(), false);
    } catch (OperationContextException e) {
      throw new RuntimeException(e);
    }
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

    try {
      return opContext.toBuilder()
          // update lineage flags for the request's session
          .searchContext(opContext.getSearchContext().withLineageFlagDefaults(flagDefaults))
          .build(opContext.getSessionActorContext(), false);
    } catch (OperationContextException e) {
      throw new RuntimeException(e);
    }
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
      @Nullable RetrieverContext retrieverContext,
      @Nonnull ValidationContext validationContext,
      @Nullable TraceContext traceContext,
      boolean enforceExistenceEnabled) {
    return asSystem(
        config,
        systemAuthentication,
        entityRegistry,
        servicesRegistryContext,
        indexConvention,
        retrieverContext,
        validationContext,
        ObjectMapperContext.DEFAULT,
        traceContext,
        enforceExistenceEnabled);
  }

  public static OperationContext asSystem(
      @Nonnull OperationContextConfig config,
      @Nonnull Authentication systemAuthentication,
      @Nullable EntityRegistry entityRegistry,
      @Nullable ServicesRegistryContext servicesRegistryContext,
      @Nullable IndexConvention indexConvention,
      @Nullable RetrieverContext retrieverContext,
      @Nonnull ValidationContext validationContext,
      @Nonnull ObjectMapperContext objectMapperContext,
      @Nullable TraceContext traceContext,
      boolean enforceExistenceEnabled) {

    ActorContext systemActorContext =
        ActorContext.builder()
            .systemAuth(true)
            .authentication(systemAuthentication)
            .enforceExistenceEnabled(enforceExistenceEnabled)
            .build();
    OperationContextConfig systemConfig =
        config.toBuilder().allowSystemAuthentication(true).build();
    SearchContext systemSearchContext =
        indexConvention == null
            ? SearchContext.EMPTY
            : SearchContext.builder().indexConvention(indexConvention).build();

    try {
      return OperationContext.builder()
          .operationContextConfig(systemConfig)
          .systemActorContext(systemActorContext)
          .searchContext(systemSearchContext)
          .entityRegistryContext(EntityRegistryContext.builder().build(entityRegistry))
          .servicesRegistryContext(servicesRegistryContext)
          // Authorizer.EMPTY doesn't actually apply to system auth
          .authorizationContext(
              AuthorizationContext.builder().authorizer(Authorizer.SYSTEM).build())
          .retrieverContext(retrieverContext)
          .objectMapperContext(objectMapperContext)
          .validationContext(validationContext)
          .traceContext(traceContext)
          .build(systemAuthentication, false);
    } catch (OperationContextException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull private final OperationContextConfig operationContextConfig;
  @Nonnull private final ActorContext sessionActorContext;
  @Nullable private final ActorContext systemActorContext;
  @Nonnull private final SearchContext searchContext;
  @Nonnull private final AuthorizationContext authorizationContext;
  @Nonnull private final EntityRegistryContext entityRegistryContext;
  @Nullable private final ServicesRegistryContext servicesRegistryContext;
  @Nullable private final RequestContext requestContext;
  @Nonnull private final RetrieverContext retrieverContext;
  @Nonnull private final ObjectMapperContext objectMapperContext;
  @Nonnull private final ValidationContext validationContext;
  @Nullable private final TraceContext traceContext;

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
      @Nonnull Authentication sessionAuthentication)
      throws ActorAccessException {
    return OperationContext.asSession(
        this,
        requestContext,
        authorizer,
        sessionAuthentication,
        getOperationContextConfig().isAllowSystemAuthentication(),
        false);
  }

  @Nonnull
  public EntityRegistry getEntityRegistry() {
    return entityRegistryContext.getEntityRegistry();
  }

  @Nonnull
  public LineageRegistry getLineageRegistry() {
    return entityRegistryContext.getLineageRegistry();
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
    return authorizationContext.getAuthorizer().getActorPeers(sessionActorContext.getActorUrn());
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

  @Nonnull
  public AspectRetriever getAspectRetriever() {
    return retrieverContext.getAspectRetriever();
  }

  /**
   * Provides a cached authorizer interface in the context of the session user
   *
   * @param privilege the requested privilege
   * @param resourceSpec the optional resource that is the target of the privilege
   * @return authorization result
   */
  @Override
  public AuthorizationResult authorize(
      @Nonnull String privilege, @Nullable EntitySpec resourceSpec) {
    return authorizationContext.authorize(getSessionActorContext(), privilege, resourceSpec);
  }

  public AuthorizationResult authorize(
      @Nonnull String privilege,
      @Nullable EntitySpec resourceSpec,
      @Nonnull Collection<EntitySpec> subResources) {
    return authorizationContext.authorize(
        getSessionActorContext(), privilege, resourceSpec, subResources);
  }

  @Nullable
  public SystemMetadata withTraceId(@Nullable SystemMetadata systemMetadata) {
    return withTraceId(systemMetadata, false);
  }

  @Nullable
  public SystemMetadata withTraceId(@Nullable SystemMetadata systemMetadata, boolean force) {
    if (systemMetadata != null && traceContext != null) {
      return traceContext.withTraceId(systemMetadata, force);
    }
    return systemMetadata;
  }

  public SystemMetadata withProducerTrace(
      String operationName, @Nullable SystemMetadata systemMetadata, String topicName) {
    if (systemMetadata != null && traceContext != null) {
      return traceContext.withProducerTrace(operationName, systemMetadata, topicName);
    }
    return systemMetadata;
  }

  /**
   * Generic method to capture spans
   *
   * @param name name of the span
   * @param operation the actual logic
   * @param attributes additional attributes
   * @return the output from the logic
   * @param <T> generic
   */
  public <T> T withSpan(String name, Supplier<T> operation, String... attributes) {
    if (traceContext != null) {
      return traceContext.withSpan(name, operation, attributes);
    } else {
      return operation.get();
    }
  }

  public void withSpan(String name, Runnable operation, String... attributes) {
    if (traceContext != null) {
      traceContext.withSpan(name, operation, attributes);
    } else {
      operation.run();
    }
  }

  public void withQueueSpan(
      String name,
      SystemMetadata systemMetadata,
      String topicName,
      Runnable operation,
      String... attributes) {
    if (systemMetadata != null) {
      withQueueSpan(name, List.of(systemMetadata), topicName, operation, attributes);
    } else {
      operation.run();
    }
  }

  public void withQueueSpan(
      String name,
      List<SystemMetadata> systemMetadata,
      String topicName,
      Runnable operation,
      String... attributes) {
    if (traceContext != null) {
      traceContext.withQueueSpan(name, systemMetadata, topicName, operation, attributes);
    } else {
      operation.run();
    }
  }

  public String traceException(Set<Throwable> throwables) {
    try {
      return getObjectMapper()
          .writeValueAsString(
              throwables.stream().map(TraceException::new).collect(Collectors.toList()));
    } catch (JsonProcessingException e) {
      log.error("Error creating trace.", e);
    }
    return throwables.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"));
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
            .add(getAuthorizationContext())
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
            .add(getRetrieverContext())
            .add(getObjectMapperContext())
            .add(getTraceContext() == null ? EmptyContext.EMPTY : getTraceContext())
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
            .add(getRetrieverContext())
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

  @Nonnull
  public ObjectMapper getYamlMapper() {
    return objectMapperContext.getYamlMapper();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OperationContext that = (OperationContext) o;
    return operationContextConfig.equals(that.operationContextConfig)
        && sessionActorContext.equals(that.sessionActorContext)
        && searchContext.equals(that.searchContext)
        && entityRegistryContext.equals(that.entityRegistryContext);
  }

  @Override
  public int hashCode() {
    int result = operationContextConfig.hashCode();
    result = 31 * result + sessionActorContext.hashCode();
    result = 31 * result + searchContext.hashCode();
    result = 31 * result + entityRegistryContext.hashCode();
    return result;
  }

  public static class OperationContextBuilder {

    @Nonnull
    public OperationContext build(
        @Nonnull Authentication sessionAuthentication, boolean enforceExistenceEnabled) {
      return build(sessionAuthentication, false, enforceExistenceEnabled);
    }

    @Nonnull
    public OperationContext build(
        @Nonnull Authentication sessionAuthentication,
        boolean skipCache,
        boolean enforceExistenceEnabled) {
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
              .policyInfoSet(this.authorizationContext.getAuthorizer().getActorPolicies(actorUrn))
              .groupMembership(this.authorizationContext.getAuthorizer().getActorGroups(actorUrn))
              .enforceExistenceEnabled(enforceExistenceEnabled)
              .build();
      return build(sessionActor, skipCache);
    }

    @Nonnull
    public OperationContext build(@Nonnull ActorContext sessionActor, boolean skipCache) {
      AspectRetriever retriever =
          skipCache
              ? this.retrieverContext.getAspectRetriever()
              : this.retrieverContext.getCachingAspectRetriever();

      if (!sessionActor.isActive(retriever)) {
        throw new ActorAccessException("Actor is not active");
      }

      return new OperationContext(
          this.operationContextConfig,
          sessionActor,
          this.systemActorContext,
          Objects.requireNonNull(this.searchContext),
          Objects.requireNonNull(this.authorizationContext),
          this.entityRegistryContext,
          this.servicesRegistryContext,
          this.requestContext,
          this.retrieverContext,
          this.objectMapperContext != null ? this.objectMapperContext : ObjectMapperContext.DEFAULT,
          this.validationContext,
          this.traceContext);
    }

    private OperationContext build() {
      return null;
    }
  }
}
