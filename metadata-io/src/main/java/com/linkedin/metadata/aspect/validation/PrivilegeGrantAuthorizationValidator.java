package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.authorization.PoliciesConfig.Privilege;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocks user privilege escalation through the aspects that grant a user roles or group membership,
 * at the aspect layer so the rule holds regardless of which API is used.
 *
 * <p>API authorization keys on {@code (ChangeType, entityType)} and never inspects the aspect name,
 * so entity-level edit rights are otherwise sufficient to write a role grant. The floors in {@link
 * #RULES} mirror what the corresponding UI mutations already demand — Manage Policies for {@code
 * roleMembership} matches {@code PolicyAuthUtils.canManagePolicies} used by batchAssignRole.
 *
 * <p>A grant to the acting actor requires the raised {@link #SELF_GRANT_FLOOR} instead of the base
 * floor, because the Asset Owners policy grants {@code EDIT_ENTITY} and {@code EDIT_GROUP_MEMBERS}
 * to resource owners with no resource filter. Ownership-derived rights must never be enough to
 * benefit yourself: a group owner could otherwise add themselves to a group carrying the Admin
 * role, or make themselves an owner of one.
 *
 * <p>{@code roleMembership} deliberately has no self check (a null {@link GrantRule#selfFloor()}).
 * An actor holding Manage Policies can already grant Admin to any other account and act as it, so
 * blocking self-assignment would add friction without security.
 *
 * <p>The resource a privilege is checked against is not always the aspect's own entity. Group
 * membership lives on the member's corpuser entity, but {@code EDIT_GROUP_MEMBERS} is held on the
 * group - so it is authorized against each added group, matching {@code
 * AuthorizationUtils.canEditGroupMembers}. Every added group must be authorized, so an editable
 * group cannot carry unauthorized ones along in the same write.
 *
 * <p>Only additions count as grants; removals, deletes, and re-ingestion of unchanged state always
 * pass. A proposal that cannot be read is treated as the worst case and requires the strictest
 * floor rather than passing unchecked.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class PrivilegeGrantAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  private static final String ROLES_LABEL = "roles";
  private static final String GROUPS_LABEL = "groups";
  private static final String OWNERS_LABEL = "owners";

  private static final String UNRESOLVABLE_MESSAGE =
      "Unauthorized to modify %s on %s (proposed value could not be resolved, so %s is required)";
  private static final String SELF_GRANT_MESSAGE =
      "Unauthorized to grant %s %s to self on %s (self grants require %s)";
  private static final String GRANT_MESSAGE = "Unauthorized to grant %s %s (requires %s on %s)";

  /** Raised floor applied when the acting actor is among the beneficiaries of the grant. */
  private static final Privilege SELF_GRANT_FLOOR =
      PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE;

  private static final Map<String, GrantRule<?>> RULES =
      Map.of(
          ROLE_MEMBERSHIP_ASPECT_NAME,
          new GrantRule<>(
              RoleMembership.class,
              membership -> orEmpty(membership.getRoles()),
              List.of(PoliciesConfig.MANAGE_POLICIES_PRIVILEGE),
              null,
              Beneficiary.ENTITY,
              AuthResource.ENTITY,
              ROLES_LABEL),
          GROUP_MEMBERSHIP_ASPECT_NAME,
          new GrantRule<>(
              GroupMembership.class,
              membership -> orEmpty(membership.getGroups()),
              List.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE, SELF_GRANT_FLOOR),
              SELF_GRANT_FLOOR,
              Beneficiary.ENTITY,
              // The privilege is held on the group, not on the member whose aspect this is.
              AuthResource.GRANTED_URNS,
              GROUPS_LABEL),
          NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
          new GrantRule<>(
              NativeGroupMembership.class,
              membership -> orEmpty(membership.getNativeGroups()),
              List.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE, SELF_GRANT_FLOOR),
              SELF_GRANT_FLOOR,
              Beneficiary.ENTITY,
              // The privilege is held on the group, not on the member whose aspect this is.
              AuthResource.GRANTED_URNS,
              GROUPS_LABEL),
          OWNERSHIP_ASPECT_NAME,
          new GrantRule<>(
              Ownership.class,
              PrivilegeGrantAuthorizationValidator::ownerUrns,
              List.of(
                  PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE,
                  PoliciesConfig.EDIT_ENTITY_PRIVILEGE,
                  SELF_GRANT_FLOOR),
              SELF_GRANT_FLOOR,
              Beneficiary.GRANTED_URNS,
              AuthResource.ENTITY,
              OWNERS_LABEL));

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    if (isInternalOperation(session)) {
      return List.of();
    }

    // Narrow to the items that need a current-state read before fetching, so a batch of unrelated
    // aspects costs nothing.
    final List<BatchItem> guarded =
        items.stream()
            .filter(item -> RULES.containsKey(item.getAspectName()))
            .filter(item -> !ChangeType.DELETE.equals(item.getChangeType()))
            .filter(item -> !isTrustedInternalWrite(item))
            .collect(Collectors.toList());
    if (guarded.isEmpty()) {
      return List.of();
    }

    final Map<Urn, Map<String, Aspect>> currentAspects =
        loadCurrentAspects(operationContext, retrieverContext.getAspectRetriever(), guarded);

    final List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : guarded) {
      validateGrant(
              RULES.get(item.getAspectName()),
              item.getAspectName(),
              operationContext,
              item,
              currentAspects,
              session)
          .ifPresent(message -> failures.add(authFailure(item, message)));
    }

    return failures;
  }

  /**
   * One read per entity type rather than one per item, so a batched membership sync does not pay a
   * serial read for every user.
   *
   * <p>URNs must be grouped by entity type: {@code
   * EntityServiceAspectRetriever.getLatestAspectObjects} scopes the query to the first URN's entity
   * type, so a single mixed call would silently return nothing for the rest — making unchanged
   * membership look like a fresh grant and denying legitimate writes.
   */
  @Nonnull
  private static Map<Urn, Map<String, Aspect>> loadCurrentAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull List<BatchItem> items) {

    final Map<String, Set<Urn>> urnsByEntityType = new HashMap<>();
    final Map<String, Set<String>> aspectNamesByEntityType = new HashMap<>();
    for (BatchItem item : items) {
      final String entityType = item.getUrn().getEntityType();
      urnsByEntityType.computeIfAbsent(entityType, key -> new HashSet<>()).add(item.getUrn());
      aspectNamesByEntityType
          .computeIfAbsent(entityType, key -> new HashSet<>())
          .add(item.getAspectName());
    }

    final Map<Urn, Map<String, Aspect>> current = new HashMap<>();
    urnsByEntityType.forEach(
        (entityType, urns) ->
            current.putAll(
                aspectRetriever.getLatestAspectObjects(
                    operationContext, urns, aspectNamesByEntityType.get(entityType))));
    return current;
  }

  @Nonnull
  private static <T extends RecordTemplate> Optional<String> validateGrant(
      @Nonnull GrantRule<T> rule,
      @Nonnull String aspectName,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull BatchItem item,
      @Nonnull Map<Urn, Map<String, Aspect>> currentAspects,
      @Nonnull AuthorizationSession session) {

    final T current = currentAspect(currentAspects, item.getUrn(), aspectName, rule);
    final ResolvedAspect<T> proposed = resolveProposed(item, current, rule.aspectClass());

    // A proposal we cannot read must not skip the check. Assume the worst payload and demand the
    // strictest floor, rather than denying a privileged actor outright.
    if (proposed.failClosed() || proposed.value() == null) {
      final Privilege strictest = strictestFloor(rule);
      if (isAnyAuthorized(session, item.getUrn(), List.of(strictest))) {
        return Optional.empty();
      }
      return Optional.of(
          String.format(UNRESOLVABLE_MESSAGE, rule.label(), item.getUrn(), strictest.getType()));
    }

    final Set<Urn> granted =
        addedUrns(
            current == null ? List.of() : rule.grantedUrns().apply(current),
            rule.grantedUrns().apply(proposed.value()));
    if (granted.isEmpty()) {
      return Optional.empty();
    }

    final Urn actor = resolveAuthorizingActor(item, operationContext);
    final boolean selfGrant =
        rule.selfFloor() != null && rule.beneficiary().includesActor(item.getUrn(), granted, actor);
    final List<Privilege> required = selfGrant ? List.of(rule.selfFloor()) : rule.baseFloor();

    // Every target must be authorized: one editable group must not carry unauthorized ones along
    // in the same write.

    final List<Urn> unauthorized =
        rule.authResource().targets(item.getUrn(), granted).stream()
            .filter(target -> !isAnyAuthorized(session, target, required))
            .collect(Collectors.toList());
    if (unauthorized.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        selfGrant
            ? String.format(
                SELF_GRANT_MESSAGE,
                rule.label(),
                granted,
                item.getUrn(),
                rule.selfFloor().getType())
            : String.format(
                GRANT_MESSAGE, rule.label(), granted, required.get(0).getType(), unauthorized));
  }

  /** The highest floor a rule can demand, used when the proposed payload cannot be determined. */
  @Nonnull
  private static Privilege strictestFloor(@Nonnull GrantRule<?> rule) {
    return rule.selfFloor() != null ? rule.selfFloor() : rule.baseFloor().get(0);
  }

  private static boolean isAnyAuthorized(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn urn,
      @Nonnull Collection<Privilege> privileges) {
    final EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), urn.toString());
    return privileges.stream()
        .anyMatch(
            privilege ->
                AuthorizationResult.Type.ALLOW.equals(
                    session.authorize(privilege.getType(), resourceSpec).getType()));
  }

  @Nonnull
  private static Set<Urn> addedUrns(
      @Nonnull Collection<Urn> current, @Nonnull Collection<Urn> proposed) {
    final Set<Urn> added = new LinkedHashSet<>(proposed);
    added.removeAll(new LinkedHashSet<>(current));
    return added;
  }

  @Nonnull
  private static Collection<Urn> orEmpty(@Nullable Collection<Urn> urns) {
    return urns == null ? List.of() : urns;
  }

  @Nonnull
  private static Collection<Urn> ownerUrns(@Nullable Ownership ownership) {
    if (ownership == null || ownership.getOwners() == null) {
      return List.of();
    }
    final Set<Urn> owners = new LinkedHashSet<>();
    for (Owner owner : ownership.getOwners()) {
      if (owner.hasOwner()) {
        owners.add(owner.getOwner());
      }
    }
    return owners;
  }

  /**
   * Session actor that initiated the write (audit stamp), not {@link
   * OperationFingerprint#getActor()} which may reflect system escalation when {@code
   * allowSystemAuthentication} is enabled on the operation context.
   */
  @Nonnull
  private static Urn resolveAuthorizingActor(
      @Nonnull BatchItem item, @Nonnull OperationFingerprint operationContext) {
    final AuditStamp itemAudit = item.getAuditStamp();
    if (itemAudit != null && itemAudit.hasActor()) {
      return itemAudit.getActor();
    }
    final AuditStamp contextAudit = operationContext.getAuditStamp();
    if (contextAudit != null && contextAudit.hasActor()) {
      return contextAudit.getActor();
    }
    return operationContext.getActor();
  }

  @Nullable
  private static <T extends RecordTemplate> T currentAspect(
      @Nonnull Map<Urn, Map<String, Aspect>> currentAspects,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull GrantRule<T> rule) {
    final Aspect aspect = currentAspects.getOrDefault(urn, Map.of()).get(aspectName);
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(rule.aspectClass(), aspect.data());
  }

  /**
   * For PATCH the full JsonPatch is applied rather than overlaying add/replace operations, so a
   * grant cannot be smuggled in through {@code move} or {@code copy}. An unresolvable patch fails
   * closed.
   */
  @Nonnull
  private static <T extends RecordTemplate> ResolvedAspect<T> resolveProposed(
      @Nonnull BatchItem item, @Nullable T current, @Nonnull Class<T> aspectClass) {
    if (!ChangeType.PATCH.equals(item.getChangeType()) || !(item instanceof MCPItem)) {
      return new ResolvedAspect<>(item.getAspect(aspectClass), false);
    }

    final JsonPatch patch = PatchOperationUtils.resolveJsonPatch((MCPItem) item);
    if (patch == null) {
      return new ResolvedAspect<>(null, true);
    }

    final T base =
        current != null ? current : RecordUtils.toRecordTemplate(aspectClass, new DataMap());
    try (JsonReader reader = Json.createReader(new StringReader(RecordUtils.toJsonString(base)))) {
      final JsonObject patched = patch.apply(reader.readObject());
      return new ResolvedAspect<>(
          RecordUtils.toRecordTemplate(aspectClass, patched.toString()), false);
    } catch (RuntimeException e) {
      log.warn(
          "Unable to apply {} PATCH for privilege check on {}; failing closed: {}",
          aspectClass.getSimpleName(),
          item.getUrn(),
          e.toString());
      return new ResolvedAspect<>(null, true);
    }
  }

  /** Bootstrap, upgrade steps, and other system-mediated writes are trusted. */
  private static boolean isTrustedInternalWrite(@Nonnull BatchItem item) {
    final SystemMetadata systemMetadata = item.getSystemMetadata();
    if (systemMetadata != null
        && systemMetadata.hasProperties()
        && SYSTEM_UPDATE_SOURCE.equals(systemMetadata.getProperties().get(APP_SOURCE))) {
      return true;
    }
    final AuditStamp auditStamp = item.getAuditStamp();
    return auditStamp != null
        && auditStamp.hasActor()
        && SYSTEM_ACTOR.equals(auditStamp.getActor().toString());
  }

  /**
   * Async proposals are authorized on the API thread before Kafka, so the MCE consumer pass (no
   * request context, or the system actor) is skipped rather than double enforced.
   */
  static boolean isInternalOperation(@Nullable AuthorizationSession session) {
    if (session == null) {
      return true;
    }
    if (!(session instanceof OperationContext)) {
      return false;
    }
    final OperationContext opContext = (OperationContext) session;
    if (opContext.getRequestContext() == null) {
      return true;
    }
    final Urn actor = opContext.getSessionActorContext().getActorUrn();
    return actor != null && SYSTEM_ACTOR.equals(actor.toString());
  }

  /** Which resource the privilege check names, independent of who benefits from the grant. */
  private enum AuthResource {
    /** The entity being written, e.g. the group whose owners are changing. */
    ENTITY {
      @Override
      Collection<Urn> targets(@Nonnull Urn entityUrn, @Nonnull Set<Urn> granted) {
        return List.of(entityUrn);
      }
    },
    /** Each newly granted URN, e.g. the groups a user is being added to. */
    GRANTED_URNS {
      @Override
      Collection<Urn> targets(@Nonnull Urn entityUrn, @Nonnull Set<Urn> granted) {
        return granted;
      }
    };

    abstract Collection<Urn> targets(@Nonnull Urn entityUrn, @Nonnull Set<Urn> granted);
  }

  /** Where the beneficiaries of a grant are named. */
  private enum Beneficiary {
    /** The entity being written to is the beneficiary, as with membership aspects. */
    ENTITY {
      @Override
      boolean includesActor(@Nonnull Urn entityUrn, @Nonnull Set<Urn> granted, @Nonnull Urn actor) {
        return entityUrn.equals(actor);
      }
    },
    /** The granted URNs are the beneficiaries, as with ownership. */
    GRANTED_URNS {
      @Override
      boolean includesActor(@Nonnull Urn entityUrn, @Nonnull Set<Urn> granted, @Nonnull Urn actor) {
        return granted.contains(actor);
      }
    };

    abstract boolean includesActor(
        @Nonnull Urn entityUrn, @Nonnull Set<Urn> granted, @Nonnull Urn actor);
  }

  /**
   * @param baseFloor privileges that authorize a grant to someone else; any one of them suffices
   * @param selfFloor privilege required when the actor is a beneficiary, or null for no self check
   * @param authResource which resource the privilege is checked against; note this is orthogonal to
   *     {@code beneficiary} and inverted for group membership versus ownership
   */
  private record GrantRule<T extends RecordTemplate>(
      @Nonnull Class<T> aspectClass,
      @Nonnull Function<T, Collection<Urn>> grantedUrns,
      @Nonnull List<Privilege> baseFloor,
      @Nullable Privilege selfFloor,
      @Nonnull Beneficiary beneficiary,
      @Nonnull AuthResource authResource,
      @Nonnull String label) {}

  private record ResolvedAspect<T extends RecordTemplate>(@Nullable T value, boolean failClosed) {}
}
