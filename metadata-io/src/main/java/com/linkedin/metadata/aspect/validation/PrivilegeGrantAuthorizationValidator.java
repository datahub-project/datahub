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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
 * <p>Only additions count as grants; removals and re-ingestion of unchanged state always pass.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class PrivilegeGrantAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  private static final String ROLES_LABEL = "roles";
  private static final String GROUPS_LABEL = "groups";
  private static final String OWNERS_LABEL = "owners";

  private static final String UNRESOLVABLE_PATCH_MESSAGE =
      "Unauthorized to modify %s on %s (proposed value could not be resolved from the patch, so %s is required)";
  private static final String SELF_GRANT_MESSAGE =
      "Unauthorized to grant %s %s to self on %s (self grants require %s)";
  private static final String GRANT_MESSAGE = "Unauthorized to grant %s %s on %s (requires %s)";

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
              ROLES_LABEL),
          GROUP_MEMBERSHIP_ASPECT_NAME,
          new GrantRule<>(
              GroupMembership.class,
              membership -> orEmpty(membership.getGroups()),
              List.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE, SELF_GRANT_FLOOR),
              SELF_GRANT_FLOOR,
              Beneficiary.ENTITY,
              GROUPS_LABEL),
          NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
          new GrantRule<>(
              NativeGroupMembership.class,
              membership -> orEmpty(membership.getNativeGroups()),
              List.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE, SELF_GRANT_FLOOR),
              SELF_GRANT_FLOOR,
              Beneficiary.ENTITY,
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

    final AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    final List<AspectValidationException> failures = new ArrayList<>();

    for (BatchItem item : items) {
      if (isTrustedInternalWrite(item)) {
        continue;
      }
      validateItem(operationContext, item, aspectRetriever, session)
          .ifPresent(message -> failures.add(authFailure(item, message)));
    }

    return failures;
  }

  @Nonnull
  private Optional<String> validateItem(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull AuthorizationSession session) {

    final GrantRule<?> rule = RULES.get(item.getAspectName());
    if (rule == null) {
      return Optional.empty();
    }
    return validateGrant(
        rule, item.getAspectName(), operationContext, item, aspectRetriever, session);
  }

  @Nonnull
  private static <T extends RecordTemplate> Optional<String> validateGrant(
      @Nonnull GrantRule<T> rule,
      @Nonnull String aspectName,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull AuthorizationSession session) {

    final T current =
        loadAspect(operationContext, aspectRetriever, item.getUrn(), aspectName, rule);
    final ResolvedAspect<T> proposed = resolveProposed(item, current, rule.aspectClass());
    if (proposed.failClosed()) {
      // Assume the worst payload so the authorization check still runs, rather than denying a
      // privileged actor outright.
      if (isAnyAuthorized(session, item.getUrn(), List.of(strictestFloor(rule)))) {
        return Optional.empty();
      }
      return Optional.of(
          String.format(
              UNRESOLVABLE_PATCH_MESSAGE,
              rule.label(),
              item.getUrn(),
              strictestFloor(rule).getType()));
    }
    if (proposed.value() == null) {
      return Optional.empty();
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
    if (isAnyAuthorized(session, item.getUrn(), required)) {
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
                GRANT_MESSAGE,
                rule.label(),
                granted,
                item.getUrn(),
                rule.baseFloor().get(0).getType()));
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
  private static <T extends RecordTemplate> T loadAspect(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull GrantRule<T> rule) {
    final Aspect aspect = aspectRetriever.getLatestAspectObject(operationContext, urn, aspectName);
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
   */
  private record GrantRule<T extends RecordTemplate>(
      @Nonnull Class<T> aspectClass,
      @Nonnull Function<T, Collection<Urn>> grantedUrns,
      @Nonnull List<Privilege> baseFloor,
      @Nullable Privilege selfFloor,
      @Nonnull Beneficiary beneficiary,
      @Nonnull String label) {}

  private record ResolvedAspect<T extends RecordTemplate>(@Nullable T value, boolean failClosed) {}
}
