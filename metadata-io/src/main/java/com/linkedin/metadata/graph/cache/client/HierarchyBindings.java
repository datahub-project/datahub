package com.linkedin.metadata.graph.cache.client;

import com.datahub.authorization.EntityFieldType;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Resolves {@link HierarchyReadSpec} instances from live cache bindings. */
public final class HierarchyBindings {

  private static final String DOMAINS_FILTER_FIELD = "domains.keyword";
  private static final String CONTAINER_FILTER_FIELD = "container.keyword";

  private HierarchyBindings() {}

  @Nonnull
  public static HierarchyReadSpec glossarySpec(@Nonnull OperationContext opContext) {
    return resolve(opContext, KnownEntityGraph.GLOSSARY)
        .orElseGet(
            () ->
                HierarchyReadSpecs.glossary(
                    EntityGraphBinding.builder()
                        .graphId(KnownEntityGraph.GLOSSARY.getConfigKey())
                        .source(KnownEntityGraph.GLOSSARY.getExpectedBuildSource())
                        .build()));
  }

  @Nonnull
  public static HierarchyReadSpec domainSpec(@Nonnull OperationContext opContext) {
    return resolveByPolicyFieldWithFallback(
            opContext, EntityFieldType.DOMAIN.name(), KnownEntityGraph.DOMAIN)
        .orElseGet(
            () ->
                HierarchyReadSpecs.domain(
                    EntityGraphBinding.builder()
                        .graphId(KnownEntityGraph.DOMAIN.getConfigKey())
                        .source(KnownEntityGraph.DOMAIN.getExpectedBuildSource())
                        .build()));
  }

  @Nonnull
  public static HierarchyReadSpec containerSpec(@Nonnull OperationContext opContext) {
    return resolveByPolicyFieldWithFallback(
            opContext, EntityFieldType.CONTAINER.name(), KnownEntityGraph.CONTAINER)
        .orElseGet(
            () ->
                HierarchyReadSpecs.container(
                    EntityGraphBinding.builder()
                        .graphId(KnownEntityGraph.CONTAINER.getConfigKey())
                        .source(KnownEntityGraph.CONTAINER.getExpectedBuildSource())
                        .build()));
  }

  @Nonnull
  public static Optional<HierarchyReadSpec> resolve(
      @Nonnull OperationContext opContext, @Nonnull KnownEntityGraph known) {
    EntityGraphCache cache = opContext.getEntityGraphCache();
    return cache
        .bindingForKnownGraph(known)
        .flatMap(binding -> HierarchyReadSpecs.forKnownGraph(known, binding));
  }

  @Nonnull
  public static Optional<HierarchyReadSpec> resolveByPolicyField(
      @Nonnull OperationContext opContext, @Nonnull String policyFieldType) {
    EntityGraphCache cache = opContext.getEntityGraphCache();
    Optional<EntityGraphBinding> binding = cache.bindingForPolicyField(policyFieldType);
    if (binding.isEmpty()) {
      return Optional.empty();
    }
    return knownGraphForPolicyField(policyFieldType)
        .flatMap(known -> HierarchyReadSpecs.forKnownGraph(known, binding.get()));
  }

  @Nonnull
  public static Optional<HierarchyReadSpec> resolveByPolicyFieldWithFallback(
      @Nonnull OperationContext opContext,
      @Nonnull String policyFieldType,
      @Nonnull KnownEntityGraph fallbackKnown) {
    return resolveByPolicyField(opContext, policyFieldType)
        .or(() -> resolve(opContext, fallbackKnown));
  }

  @Nonnull
  public static Optional<HierarchyReadSpec> resolveByFilterField(
      @Nonnull OperationContext opContext, @Nonnull String filterField) {
    EntityGraphCache cache = opContext.getEntityGraphCache();
    Optional<EntityGraphBinding> binding = cache.bindingForFilterField(filterField);
    if (binding.isEmpty()) {
      return Optional.empty();
    }
    return knownGraphForFilterField(filterField)
        .flatMap(known -> HierarchyReadSpecs.forKnownGraph(known, binding.get()));
  }

  @Nonnull
  public static Optional<HierarchyReadSpec> resolveByFilterFieldWithFallback(
      @Nonnull OperationContext opContext,
      @Nonnull String filterField,
      @Nonnull KnownEntityGraph fallbackKnown) {
    return resolveByFilterField(opContext, filterField).or(() -> resolve(opContext, fallbackKnown));
  }

  @Nonnull
  private static Optional<KnownEntityGraph> knownGraphForPolicyField(
      @Nonnull String policyFieldType) {
    if (EntityFieldType.DOMAIN.name().equals(policyFieldType)) {
      return Optional.of(KnownEntityGraph.DOMAIN);
    }
    if (EntityFieldType.CONTAINER.name().equals(policyFieldType)) {
      return Optional.of(KnownEntityGraph.CONTAINER);
    }
    return Optional.empty();
  }

  @Nonnull
  private static Optional<KnownEntityGraph> knownGraphForFilterField(@Nonnull String filterField) {
    if (DOMAINS_FILTER_FIELD.equals(filterField)) {
      return Optional.of(KnownEntityGraph.DOMAIN);
    }
    if (CONTAINER_FILTER_FIELD.equals(filterField)) {
      return Optional.of(KnownEntityGraph.CONTAINER);
    }
    return Optional.empty();
  }
}
