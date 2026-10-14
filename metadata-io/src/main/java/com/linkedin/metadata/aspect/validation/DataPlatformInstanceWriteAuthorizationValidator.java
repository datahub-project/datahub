package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.DomainWriteAuthorizationUtils;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Authorizes changes to an entity's {@code dataPlatformInstance}, mirroring {@link
 * DomainWriteAuthorizationValidator}. Policies can scope writes by the entity's stored platform
 * instance, while ingest authorization only checks the URN. Without this check, a writer could move
 * an entity into an instance it has no rights on.
 *
 * <p>When the instance changes, the writer must be allowed to write the entity as it is now (stored
 * or earlier-in-batch instance), if it has one, and as it would be with the proposed instance, if
 * one is set. The privilege is Create or Edit Entity depending on whether the entity exists, as for
 * domains. The proposed value is evaluated through a request-scoped {@code DATA_PLATFORM_INSTANCE}
 * field override, which is removed again after the check.
 *
 * <ul>
 *   <li>Unchanged instance: no authorization calls.
 *   <li>Setting a first instance (including on entity creation): checked against the proposed
 *       instance only.
 *   <li>Removing the instance: checked against the current instance only. Requiring a match on "no
 *       instance" would stop instance-scoped owners from ever clearing it.
 *   <li>A proposed value that cannot be resolved (e.g. a PATCH that does not apply): denied.
 * </ul>
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class DataPlatformInstanceWriteAuthorizationValidator
    extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {
    List<BatchItem> instanceItems =
        items.stream()
            .filter(item -> DATA_PLATFORM_INSTANCE_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());
    if (instanceItems.isEmpty()) {
      return List.of();
    }

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    Set<Urn> urns = instanceItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    // Updated in batch order so a later item is judged against an earlier one for the same URN.
    Map<Urn, DataPlatformInstance> current = loadStored(operationContext, aspectRetriever, urns);

    // Read only once an item actually changes the instance; most ingestion re-writes the same one.
    Map<Urn, Boolean> entityExists = null;
    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : instanceItems) {
      Urn urn = item.getUrn();
      DataPlatformInstance before = current.get(urn);
      DataPlatformInstance after = resolveProposed(item, aspectRetriever, before);
      if (after == null) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to change dataPlatformInstance on entity "
                    + urn
                    + " (could not resolve the proposed value)"));
        continue;
      }
      current.put(urn, after);

      Urn beforeInstance = before == null ? null : before.getInstance();
      Urn afterInstance = after.getInstance();
      if (Objects.equals(beforeInstance, afterInstance)) {
        continue;
      }
      if (entityExists == null) {
        entityExists =
            DomainWriteAuthorizationUtils.resolveEntityExists(
                operationContext, aspectRetriever, urns);
      }
      ApiOperation operation =
          DomainWriteAuthorizationUtils.resolveApiOperation(
              item.getChangeType(), Boolean.TRUE.equals(entityExists.get(urn)));
      if ((beforeInstance != null
              && !isAuthorizedWithInstance(session, urn, beforeInstance, operation))
          || (afterInstance != null
              && !isAuthorizedWithInstance(session, urn, afterInstance, operation))) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to change dataPlatformInstance on entity "
                    + urn
                    + " (requires CREATE_ENTITY or EDIT_ENTITY for both the current and the"
                    + " proposed instance)"));
      }
    }
    return failures;
  }

  /**
   * Evaluates the entity {@code operation} privilege as if the entity had {@code instance}. Other
   * stored fields (domains, container, tags, ...) still apply; any override already recorded for
   * the resource, such as proposed domains, is kept and restored afterwards.
   */
  private static boolean isAuthorizedWithInstance(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn urn,
      @Nonnull Urn instance,
      @Nonnull ApiOperation operation) {
    if (!(session instanceof OperationContext)) {
      // A proposed instance can only be evaluated through a request context; fail closed.
      return false;
    }
    Map<EntitySpec, Map<EntityFieldType, FieldResolver>> overrides =
        ((OperationContext) session).getAuthorizationContext().getSessionResourceFieldOverrides();
    EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), urn.toString());
    Map<EntityFieldType, FieldResolver> previous = overrides.get(resourceSpec);
    Map<EntityFieldType, FieldResolver> merged = new EnumMap<>(EntityFieldType.class);
    if (previous != null) {
      merged.putAll(previous);
    }
    merged.put(
        EntityFieldType.DATA_PLATFORM_INSTANCE,
        FieldResolver.getResolverFromValues(Set.of(instance.toString())));
    overrides.put(resourceSpec, merged);
    try {
      return AuthUtil.isAuthorized(
          session,
          AuthUtil.buildDisjunctivePrivilegeGroup(
              AuthUtil.lookupAPIPrivilege(ENTITY, operation, urn.getEntityType())),
          resourceSpec);
    } finally {
      if (previous == null) {
        overrides.remove(resourceSpec);
      } else {
        overrides.put(resourceSpec, previous);
      }
    }
  }

  /** Proposed value for one item, with PATCH applied to {@code current}; null if unresolvable. */
  @Nullable
  private static DataPlatformInstance resolveProposed(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nullable DataPlatformInstance current) {
    try {
      return PatchItemImpl.resolveProposedAspect(
          item, current, aspectRetriever, DataPlatformInstance.class);
    } catch (Exception e) {
      log.warn(
          "Failed to resolve proposed dataPlatformInstance for urn={}: {}",
          item.getUrn(),
          e.toString());
      return null;
    }
  }

  /** One read per entity type: the aspect retriever scopes a lookup to the first URN's type. */
  @Nonnull
  private static Map<Urn, DataPlatformInstance> loadStored(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Set<Urn> urns) {
    Map<Urn, DataPlatformInstance> result = new HashMap<>();
    Map<String, Set<Urn>> byEntityType =
        urns.stream()
            .collect(
                Collectors.groupingBy(Urn::getEntityType, Collectors.toCollection(HashSet::new)));
    for (Set<Urn> typeUrns : byEntityType.values()) {
      Map<Urn, Map<String, Aspect>> latest =
          aspectRetriever.getLatestAspectObjects(
              operationContext, typeUrns, Set.of(DATA_PLATFORM_INSTANCE_ASPECT_NAME));
      for (Urn urn : typeUrns) {
        Aspect aspect = latest.getOrDefault(urn, Map.of()).get(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
        if (aspect != null) {
          result.put(urn, RecordUtils.toRecordTemplate(DataPlatformInstance.class, aspect.data()));
        }
      }
    }
    return result;
  }
}
