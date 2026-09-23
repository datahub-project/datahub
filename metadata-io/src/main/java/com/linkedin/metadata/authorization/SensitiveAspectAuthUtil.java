package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.CORP_USER_CREDENTIALS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Read authorization for versioned aspects that carry credential material. Entity READ is decided
 * per entity; these aspects additionally require the mapped platform privilege, regardless of how
 * widely the owning entity itself may be viewed. Applied on every API surface that returns raw
 * aspect payloads (GraphQL {@code aspects}, OpenAPI, Rest.li).
 */
public final class SensitiveAspectAuthUtil {

  private static final Map<Pair<String, String>, PoliciesConfig.Privilege> SENSITIVE_ASPECTS =
      Map.of(
          Pair.of(CORP_USER_ENTITY_NAME, CORP_USER_CREDENTIALS_ASPECT_NAME),
          PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE);

  private SensitiveAspectAuthUtil() {}

  /**
   * True when the actor may read the given aspect of the given entity type. Non-sensitive aspects
   * are always readable here (entity READ is enforced by the caller); sensitive aspects require the
   * mapped privilege. System principals always pass.
   */
  public static boolean canReadAspect(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nullable String aspectName) {
    if (opContext.isSystemAuth()) {
      return true;
    }
    Optional<PoliciesConfig.Privilege> required = sensitivePrivilege(entityName, aspectName);
    return required.isEmpty() || AuthUtil.isAuthorized(opContext, required.get());
  }

  public static boolean canReadAspect(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String aspectName) {
    return canReadAspect(opContext, urn.getEntityType(), aspectName);
  }

  /** Drop sensitive aspects the actor may not read. Keys are aspect names on a single entity. */
  @Nonnull
  public static <V> Map<String, V> omitUnauthorizedAspects(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull Map<String, V> aspects) {
    if (aspects.isEmpty() || !hasSensitiveAspects(urn.getEntityType())) {
      return aspects;
    }
    Map<String, V> allowed = new LinkedHashMap<>();
    for (Map.Entry<String, V> entry : aspects.entrySet()) {
      if (canReadAspect(opContext, urn, entry.getKey())) {
        allowed.put(entry.getKey(), entry.getValue());
      }
    }
    return allowed;
  }

  /**
   * Remove sensitive aspects the actor may not read from an enveloped entity response. The input is
   * never mutated: the service may cache or share the instance, and a later caller holding the
   * privilege must still see the aspect. Returns the same instance when nothing is filtered.
   */
  @Nullable
  public static EntityResponse omitUnauthorizedAspects(
      @Nonnull OperationContext opContext, @Nullable EntityResponse response) {
    if (response == null
        || !response.hasUrn()
        || !response.hasAspects()
        || !hasSensitiveAspects(response.getUrn().getEntityType())) {
      return response;
    }
    EnvelopedAspectMap allowed = new EnvelopedAspectMap();
    response
        .getAspects()
        .forEach(
            (name, aspect) -> {
              if (canReadAspect(opContext, response.getUrn(), name)) {
                allowed.put(name, aspect);
              }
            });
    if (allowed.size() == response.getAspects().size()) {
      return response;
    }
    try {
      EntityResponse filtered =
          RecordUtils.toRecordTemplate(EntityResponse.class, response.data().copy());
      filtered.setAspects(allowed);
      return filtered;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy entity response", e);
    }
  }

  /**
   * Map form of {@link #omitUnauthorizedAspects(OperationContext, EntityResponse)}; returns a new
   * map.
   */
  @Nonnull
  public static Map<Urn, EntityResponse> omitUnauthorizedAspects(
      @Nonnull OperationContext opContext, @Nonnull Map<Urn, EntityResponse> responses) {
    Map<Urn, EntityResponse> filtered = new LinkedHashMap<>();
    responses.forEach(
        (urn, response) -> filtered.put(urn, omitUnauthorizedAspects(opContext, response)));
    return filtered;
  }

  private static boolean hasSensitiveAspects(@Nullable String entityName) {
    return entityName != null
        && SENSITIVE_ASPECTS.keySet().stream()
            .anyMatch(key -> key.getFirst().equalsIgnoreCase(entityName));
  }

  private static Optional<PoliciesConfig.Privilege> sensitivePrivilege(
      @Nullable String entityName, @Nullable String aspectName) {
    if (StringUtils.isBlank(entityName) || StringUtils.isBlank(aspectName)) {
      return Optional.empty();
    }
    // Aspect names are matched case-insensitively because OpenAPI path segments are.
    return SENSITIVE_ASPECTS.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().getFirst().equalsIgnoreCase(entityName)
                    && entry.getKey().getSecond().equalsIgnoreCase(aspectName))
        .map(Map.Entry::getValue)
        .findFirst();
  }
}
