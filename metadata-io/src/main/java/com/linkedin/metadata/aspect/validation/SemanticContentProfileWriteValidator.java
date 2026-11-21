package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * While generation is on, a service-owned profile's vectors change only through apply. Marker
 * fields ({@code current}, {@code refreshMarker}, {@code dirty}) stay writable so a content edit
 * can record that the vectors are stale.
 */
@Setter
@Getter
@Accessors(chain = true)
public class SemanticContentProfileWriteValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;
  @Nonnull private SemanticProfileRegistry registry;

  /**
   * The Search V3 projector drops a profile whose model is not configured, so storing one leaves
   * vectors the index never receives. V2-only deployments keep accepting any key.
   */
  private boolean requireConfiguredProfiles;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return changeMCPs.stream().flatMap(item -> violations(item, registry));
  }

  @Nonnull
  static Set<String> violationMessages(
      @Nonnull String entityType,
      @Nullable SemanticContent previous,
      @Nullable SemanticContent proposed,
      boolean delete,
      boolean apply,
      boolean requireConfigured,
      @Nonnull SemanticProfileRegistry registry) {
    Set<String> messages = new HashSet<>();
    if (proposed != null && proposed.hasEmbeddings()) {
      proposed
          .getEmbeddings()
          .forEach((key, profile) -> messages.addAll(stateMessages(key, profile)));
    }
    if (requireConfigured && !delete) {
      messages.addAll(unconfiguredMessages(entityType, previous, proposed, registry));
    }
    if (!registry.generationEnabled() || apply) {
      return messages;
    }
    Set<String> before = keys(previous);
    Set<String> after = delete || proposed == null ? Set.of() : keys(proposed);
    for (String key : before) {
      if (!registry.isServiceProfile(entityType, key)) {
        continue;
      }
      if (!after.contains(key)) {
        messages.add("service profile " + key + " cannot be removed without apply");
        continue;
      }
      if (vectorsChanged(previous.getEmbeddings().get(key), proposed.getEmbeddings().get(key))) {
        messages.add("service profile " + key + " vectors cannot change without apply");
      }
    }
    for (String key : after) {
      if (registry.isServiceProfile(entityType, key) && !before.contains(key)) {
        messages.add("service profile " + key + " cannot be added without apply");
      }
    }
    return messages;
  }

  /**
   * A retired model's profile stays stored, and a source edit or refresh marker still touches it,
   * so only adding the key or changing its vectors is rejected. Removing it is allowed.
   */
  @Nonnull
  private static Set<String> unconfiguredMessages(
      @Nonnull String entityType,
      @Nullable SemanticContent previous,
      @Nullable SemanticContent proposed,
      @Nonnull SemanticProfileRegistry registry) {
    Set<String> messages = new HashSet<>();
    for (String key : keys(proposed)) {
      if (registry.isConfigured(entityType, key)) {
        continue;
      }
      if (!keys(previous).contains(key)) {
        messages.add("profile " + key + " is not configured in semanticSearch.models");
      } else if (vectorsChanged(
          previous.getEmbeddings().get(key), proposed.getEmbeddings().get(key))) {
        messages.add("profile " + key + " is not configured; its vectors cannot change");
      }
    }
    return messages;
  }

  @Nonnull
  private static Set<String> stateMessages(
      @Nonnull String key, @Nonnull EmbeddingModelData profile) {
    Set<String> messages = new HashSet<>();
    if (!profile.hasVectorState()) {
      return messages;
    }
    boolean empty = !profile.hasChunks() || profile.getChunks().isEmpty();
    if (profile.getVectorState() == SemanticVectorState.READY && empty) {
      messages.add("profile " + key + " READY requires chunks");
    }
    if ((profile.getVectorState() == SemanticVectorState.EXCLUDED
            || profile.getVectorState() == SemanticVectorState.SKIPPED)
        && !empty) {
      messages.add("profile " + key + " " + profile.getVectorState() + " requires empty chunks");
    }
    if ((profile.getVectorState() == SemanticVectorState.EXCLUDED
            || profile.getVectorState() == SemanticVectorState.SKIPPED)
        && (!profile.hasStateReason() || profile.getStateReason().isBlank())) {
      messages.add("profile " + key + " " + profile.getVectorState() + " requires a reason");
    }
    return messages;
  }

  private static boolean vectorsChanged(
      @Nonnull EmbeddingModelData before, @Nonnull EmbeddingModelData after) {
    return !Objects.equals(before.getChunks(), after.getChunks())
        || !Objects.equals(before.getContentHash(), after.getContentHash())
        || !Objects.equals(before.getSourceTextSha256(), after.getSourceTextSha256())
        || !Objects.equals(before.getVectorState(), after.getVectorState())
        || !Objects.equals(before.getWorkState(), after.getWorkState())
        || !Objects.equals(before.getStateReason(), after.getStateReason())
        || !Objects.equals(before.getPolicyVersion(), after.getPolicyVersion())
        || !Objects.equals(before.getVectorDimension(), after.getVectorDimension())
        || !Objects.equals(before.getModelVersion(), after.getModelVersion());
  }

  @Nonnull
  private static Set<String> keys(@Nullable SemanticContent content) {
    if (content == null || !content.hasEmbeddings()) {
      return Set.of();
    }
    return content.getEmbeddings().keySet();
  }

  private static boolean isApply(@Nonnull ChangeMCP item) {
    return item.getSystemMetadata() != null
        && item.getSystemMetadata().hasProperties()
        && item.getSystemMetadata().getProperties().containsKey(SEMANTIC_APPLY_PROPERTY)
        && item.getAuditStamp() != null
        && item.getAuditStamp().hasActor()
        && SYSTEM_ACTOR.equals(item.getAuditStamp().getActor().toString());
  }

  @Nonnull
  private Stream<AspectValidationException> violations(
      @Nonnull ChangeMCP item, @Nonnull SemanticProfileRegistry registry) {
    return violationMessages(
            item.getUrn().getEntityType(),
            item.getPreviousAspect(SemanticContent.class),
            item.getChangeType() == ChangeType.DELETE
                ? null
                : item.getAspect(SemanticContent.class),
            item.getChangeType() == ChangeType.DELETE,
            isApply(item),
            requireConfiguredProfiles,
            registry)
        .stream()
        .map(message -> AspectValidationException.forItem(item, message));
  }
}
