package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.metadata.AcrylConstants.*;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.FreshnessFieldKind;
import com.linkedin.assertion.FreshnessFieldSpec;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersInput;
import com.linkedin.datahub.graphql.generated.AuditLogSpecInput;
import com.linkedin.datahub.graphql.generated.CronScheduleInput;
import com.linkedin.datahub.graphql.generated.DataHubOperationSpecInput;
import com.linkedin.datahub.graphql.generated.DatasetFieldAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetSchemaAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.FreshnessFieldSpecInput;
import com.linkedin.datahub.graphql.generated.SystemMonitorType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DataHubOperationSpec;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFieldAssertionSourceType;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.monitor.DatasetSchemaAssertionParameters;
import com.linkedin.monitor.DatasetSchemaSourceType;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MonitorUtils {

  private static final String MONITORS_RELATIONSHIP_NAME = "Monitors";
  private static final int MAX_MONITORS_TO_FETCH = 10000;

  // Entity types that have system monitors enabled.
  public static final Set<String> ENTITY_TYPES_WITH_SYSTEM_MONITORS =
      ImmutableSet.of(Constants.DATASET_ENTITY_NAME);
  private static final String EVALUATES_RELATIONSHIP_NAME = "Evaluates";

  /**
   * Converts the URN for an entity and the type of a system monitor into a unique monitor urn,
   * using a pre-defined convention.
   *
   * <p>For each monitor, the Monitor Urn is composed of 2 parts: a) an entity urn, and b) a unique
   * monitor id.
   *
   * <p>For system monitors, we simply use a reserved set of "system" ids for each system monitor
   * type that is supported. For the full list, see {@link com.linkedin.metadata.AcrylConstants}.
   */
  public static Urn getMonitorUrnForSystemMonitorType(
      @Nonnull final Urn entityUrn, @Nonnull final SystemMonitorType type) {
    final MonitorKey key = new MonitorKey();
    key.setEntity(entityUrn);
    key.setId(MonitorUtils.getIdForSystemMonitorType(type));
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.MONITOR_ENTITY_NAME);
  }

  /** Converts the system monitor type into a monitor id that is unique for the specified entity. */
  public static String getIdForSystemMonitorType(@Nonnull final SystemMonitorType type) {
    switch (type) {
      case FRESHNESS:
        return FRESHNESS_SYSTEM_MONITOR_ID;
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized SystemMonitorType %s provided!", type));
    }
  }

  /**
   * Determine whether the current user is allowed to change an entity's monitors.
   *
   * <p>This is determined by either having the MANAGE_MONITORS platform privilege, to edit monitors
   * for all assets, or the EDIT_MONITORs entity privilege for the target entity.
   */
  public static boolean isAuthorizedToUpdateEntityMonitors(
      @Nonnull final Urn entityUrn, @Nonnull final QueryContext context) {
    // For monitors, you must explicitly be granted the privilege to create them.
    // The classic ALL ENTITY privilege will not get you access, because of their heightened
    // sensitivity.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_MONITORS.getType()))));
    return AuthUtil.isAuthorized(
            context.getOperationContext(),
            PoliciesConfig.MANAGE_MONITORS,
            new EntitySpec(entityUrn.getEntityType(), entityUrn.toString()))
        || AuthorizationUtils.isAuthorized(
            context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }

  /**
   * Determine whether the current user is allowed to update a SQL assertion monitor for a given
   * entity.
   *
   * <p>SQL assertion monitors allow users to run arbitrary SQL, and are thus controlled more
   * tightly than the other types of monitors.
   */
  public static boolean isAuthorizedToUpdateSqlAssertionMonitors(
      @Nonnull final Urn entityUrn, @Nonnull final QueryContext context) {
    // For SQL assertion monitors, you must explicitly be granted the privilege to create them.
    // The classic ALL ENTITY privilege will not get you access.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.EDIT_ENTITY_SQL_ASSERTION_MONITORS.getType()))));
    return AuthorizationUtils.isAuthorized(
            context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups)
        || AuthUtil.isAuthorized(
            context.getOperationContext(),
            PoliciesConfig.MANAGE_MONITORS,
            new EntitySpec(entityUrn.getEntityType(), entityUrn.toString()));
  }

  public static CronSchedule createCronSchedule(@Nonnull final CronScheduleInput input) {
    final CronSchedule result = new CronSchedule();
    result.setCron(input.getCron());
    result.setTimezone(input.getTimezone());
    return result;
  }

  public static AssertionEvaluationParameters createAssertionEvaluationParameters(
      @Nonnull final AssertionEvaluationParametersInput input) {
    final AssertionEvaluationParameters result = new AssertionEvaluationParameters();
    result.setType(AssertionEvaluationParametersType.valueOf(input.getType().toString()));

    if (AssertionEvaluationParametersType.DATASET_FRESHNESS.equals(result.getType())) {
      if (input.getDatasetFreshnessParameters() != null) {
        result.setDatasetFreshnessParameters(
            createDatasetFreshnessParameters(input.getDatasetFreshnessParameters()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Dataset Freshness Parameters are required when type is DATASET_FRESHNESS.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    if (AssertionEvaluationParametersType.DATASET_VOLUME.equals(result.getType())) {
      if (input.getDatasetVolumeParameters() != null) {
        result.setDatasetVolumeParameters(
            createDatasetVolumeParameters(input.getDatasetVolumeParameters()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Dataset Volume Parameters are required when type is DATASET_VOLUME.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    if (AssertionEvaluationParametersType.DATASET_FIELD.equals(result.getType())) {
      if (input.getDatasetFieldParameters() != null) {
        result.setDatasetFieldParameters(
            createDatasetFieldParameters(input.getDatasetFieldParameters()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Dataset Field Parameters are required when type is DATASET_FIELD.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    if (AssertionEvaluationParametersType.DATASET_SCHEMA.equals(result.getType())) {
      if (input.getDatasetSchemaParameters() != null) {
        result.setDatasetSchemaParameters(
            createDatasetSchemaParameters(input.getDatasetSchemaParameters()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Dataset Schema Parameters are required when type is DATASET_SCHEMA.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }

    return result;
  }

  public static DatasetFreshnessAssertionParameters createDatasetFreshnessParameters(
      @Nonnull final DatasetFreshnessAssertionParametersInput input) {
    final DatasetFreshnessAssertionParameters result = new DatasetFreshnessAssertionParameters();
    result.setSourceType(DatasetFreshnessSourceType.valueOf(input.getSourceType().toString()));
    if (DatasetFreshnessSourceType.AUDIT_LOG.equals(result.getSourceType())) {
      if (input.getAuditLog() != null) {
        result.setAuditLog(createAuditLogSpec(input.getAuditLog()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Audit Log info is required if type FRESHNESS source type is AUDIT_LOG.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    if (DatasetFreshnessSourceType.FIELD_VALUE.equals(result.getSourceType())) {
      if (input.getField() != null) {
        result.setField(createFreshnessFieldSpec(input.getField()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Field info is required if type FRESHNESS source type is FIELD_VALUE.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    if (DatasetFreshnessSourceType.DATAHUB_OPERATION.equals(result.getSourceType())) {
      if (input.getDataHubOperation() != null) {
        result.setDataHubOperation(createDataHubOperationSpec(input.getDataHubOperation()));
      }
    }
    return result;
  }

  public static DatasetVolumeAssertionParameters createDatasetVolumeParameters(
      @Nonnull final DatasetVolumeAssertionParametersInput input) {
    final DatasetVolumeAssertionParameters result = new DatasetVolumeAssertionParameters();
    result.setSourceType(DatasetVolumeSourceType.valueOf(input.getSourceType().toString()));
    return result;
  }

  public static DatasetFieldAssertionParameters createDatasetFieldParameters(
      @Nonnull final DatasetFieldAssertionParametersInput input) {
    final DatasetFieldAssertionParameters result = new DatasetFieldAssertionParameters();
    result.setSourceType(DatasetFieldAssertionSourceType.valueOf(input.getSourceType().toString()));
    if (DatasetFieldAssertionSourceType.CHANGED_ROWS_QUERY.equals(result.getSourceType())
        && input.getChangedRowsField() != null) {
      result.setChangedRowsField(createFreshnessFieldSpec(input.getChangedRowsField()));
    }
    return result;
  }

  private static DatasetSchemaAssertionParameters createDatasetSchemaParameters(
      @Nonnull final DatasetSchemaAssertionParametersInput input) {
    final DatasetSchemaAssertionParameters result = new DatasetSchemaAssertionParameters();
    result.setSourceType(DatasetSchemaSourceType.valueOf(input.getSourceType().toString()));
    return result;
  }

  public static AuditLogSpec createAuditLogSpec(@Nonnull final AuditLogSpecInput input) {
    final AuditLogSpec result = new AuditLogSpec();
    if (input.getOperationTypes() != null) {
      result.setOperationTypes(new StringArray(input.getOperationTypes()));
    }
    if (input.getUserName() != null) {
      result.setUserName(input.getUserName());
    }
    return result;
  }

  public static FreshnessFieldSpec createFreshnessFieldSpec(
      @Nonnull final FreshnessFieldSpecInput input) {
    final FreshnessFieldSpec result = new FreshnessFieldSpec();
    result.setType(input.getType());
    result.setNativeType(input.getNativeType(), SetMode.IGNORE_NULL);
    result.setPath(input.getPath(), SetMode.IGNORE_NULL);

    if (input.getKind() != null) {
      result.setKind(FreshnessFieldKind.valueOf(input.getKind().toString()));
    } else {
      throw new DataHubGraphQLException(
          "Invalid input. Freshness Field Kind info is required if type Freshness source type is FIELD_VALUE.",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    return result;
  }

  public static DataHubOperationSpec createDataHubOperationSpec(
      @Nonnull final DataHubOperationSpecInput input) {
    final DataHubOperationSpec result = new DataHubOperationSpec();
    if (input.getOperationTypes() != null) {
      result.setOperationTypes(new StringArray(input.getOperationTypes()));
    }
    if (input.getCustomOperationTypes() != null) {
      result.setCustomOperationTypes(new StringArray(input.getCustomOperationTypes()));
    }
    return result;
  }

  private MonitorUtils() {}

  public static @Nonnull AssertionEvaluationParameters createFreshnessAssertionEvaluationParameters(
      @Nonnull DatasetFreshnessAssertionParametersInput datasetFreshnessParameters) {
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    parameters.setType(AssertionEvaluationParametersType.DATASET_FRESHNESS);
    parameters.setDatasetFreshnessParameters(
        createDatasetFreshnessParameters(datasetFreshnessParameters));
    return parameters;
  }

  public static @Nonnull AssertionEvaluationParameters createVolumeAssertionEvaluationParameters(
      @Nonnull DatasetVolumeAssertionParametersInput datasetVolumeParameters) {
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    parameters.setType(AssertionEvaluationParametersType.DATASET_VOLUME);
    parameters.setDatasetVolumeParameters(createDatasetVolumeParameters(datasetVolumeParameters));
    return parameters;
  }

  public static @Nonnull AssertionEvaluationParameters createFieldAssertionEvaluationParameters(
      @Nonnull DatasetFieldAssertionParametersInput datasetFieldParameters) {
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    parameters.setType(AssertionEvaluationParametersType.DATASET_FIELD);
    parameters.setDatasetFieldParameters(createDatasetFieldParameters(datasetFieldParameters));
    return parameters;
  }

  public static @Nonnull Urn getMonitorUrnForAssertionOrThrow(
      GraphClient graphClient, Urn assertionUrn) {
    Urn monitorUrnForAssertion = getMonitorUrnForAssertion(graphClient, assertionUrn);

    if (monitorUrnForAssertion == null) {
      throw new RuntimeException(
          String.format(
              "Failed to upsert Assertion. Monitor for assertion %s does not exist.",
              assertionUrn));
    }
    return monitorUrnForAssertion;
  }

  public static @Nullable Urn getMonitorUrnForAssertion(GraphClient graphClient, Urn assertionUrn) {
    final EntityRelationships relationships =
        graphClient.getRelatedEntities(
            assertionUrn.toString(),
            ImmutableList.of(EVALUATES_RELATIONSHIP_NAME),
            RelationshipDirection.INCOMING,
            0,
            1,
            null);

    final List<Urn> monitorUrns =
        relationships.getRelationships().stream()
            .map(EntityRelationship::getEntity)
            .collect(Collectors.toList());
    if (!monitorUrns.isEmpty()) {
      return monitorUrns.get(0);
    }
    return null;
  }

  public static @Nullable Urn getAssertionUrnForMonitor(GraphClient graphClient, Urn monitorUrn) {
    final EntityRelationships relationships =
        graphClient.getRelatedEntities(
            monitorUrn.toString(),
            ImmutableList.of(EVALUATES_RELATIONSHIP_NAME),
            RelationshipDirection.OUTGOING,
            0,
            1,
            null);

    final List<Urn> assertionUrns =
        relationships.getRelationships().stream()
            .map(EntityRelationship::getEntity)
            .collect(Collectors.toList());
    if (!assertionUrns.isEmpty()) {
      return assertionUrns.get(0);
    }
    return null;
  }

  public static List<Urn> getMonitorUrnsForDataset(GraphClient graphClient, Urn datasetUrn) {
    final EntityRelationships relationships =
        graphClient.getRelatedEntities(
            datasetUrn.toString(),
            ImmutableList.of(MONITORS_RELATIONSHIP_NAME),
            RelationshipDirection.INCOMING,
            0,
            MAX_MONITORS_TO_FETCH,
            null);

    return relationships.getRelationships().stream()
        .map(EntityRelationship::getEntity)
        .collect(Collectors.toList());
  }
}
