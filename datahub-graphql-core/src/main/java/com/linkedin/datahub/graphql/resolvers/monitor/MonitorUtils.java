package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.SystemMonitorType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.key.MonitorKey;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;
import static com.linkedin.metadata.AcrylConstants.*;


public class MonitorUtils {

  // Entity types that have system monitors enabled.
  public static final Set<String> ENTITY_TYPES_WITH_SYSTEM_MONITORS = ImmutableSet.of(
      Constants.DATASET_ENTITY_NAME
  );

  /**
   * Converts the URN for an entity and the type of a system monitor into a unique monitor urn, using a pre-defined
   * convention.
   *
   * For each monitor, the Monitor Urn is composed of 2 parts: a) an entity urn, and b) a unique monitor id.
   *
   * For system monitors, we simply use a reserved set of "system" ids for each system monitor type that is supported.
   * For the full list, see {@link com.linkedin.metadata.AcrylConstants}.
   */
  public static Urn getMonitorUrnForSystemMonitorType(@Nonnull final Urn entityUrn, @Nonnull final SystemMonitorType type) {
    final MonitorKey key = new MonitorKey();
    key.setEntity(entityUrn);
    key.setId(MonitorUtils.getIdForSystemMonitorType(type));
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.MONITOR_ENTITY_NAME);
  }

  /**
   * Converts the system monitor type into a monitor id that is unique for the specified entity.
   */
  public static String getIdForSystemMonitorType(@Nonnull final SystemMonitorType type) {
    switch (type) {
      case FRESHNESS:
        return FRESHNESS_SYSTEM_MONITOR_ID;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized SystemMonitorType %s provided!", type));
    }
  }

  /**
   * Determine whether the current user is allowed to change an entity's monitors.
   *
   * This is determined by either having the MANAGE_MONITORS platform privilege, to edit
   * monitors for all assets, or the EDIT_MONITORs entity privilege for the target entity.
   */
  public static boolean isAuthorizedToUpdateEntityMonitors(
      @Nonnull final Urn entityUrn,
      @Nonnull final QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(
        ImmutableList.of(ALL_PRIVILEGES_GROUP,
            new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_MONITORS_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_MONITORS)
        || AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  private MonitorUtils() { }

}
