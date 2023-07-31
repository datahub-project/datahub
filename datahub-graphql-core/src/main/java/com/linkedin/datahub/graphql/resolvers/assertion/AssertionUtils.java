package com.linkedin.datahub.graphql.resolvers.assertion;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActionType;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleInput;
import com.linkedin.datahub.graphql.resolvers.AuthUtils;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.timeseries.CalendarInterval;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AssertionUtils {

  @Nonnull
  public static com.linkedin.assertion.AssertionStdParameters createDatasetAssertionParameters(@Nonnull final AssertionStdParametersInput parameters) {
    final com.linkedin.assertion.AssertionStdParameters result = new com.linkedin.assertion.AssertionStdParameters();
    if (parameters.getValue() != null) {
      result.setValue(createDatasetAssertionParameter(parameters.getValue()));
    }
    if (parameters.getMaxValue() != null) {
      result.setMaxValue(createDatasetAssertionParameter(parameters.getMaxValue()));
    }
    if (parameters.getMinValue() != null) {
      result.setMinValue(createDatasetAssertionParameter(parameters.getMinValue()));
    }
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.AssertionStdParameter createDatasetAssertionParameter(@Nonnull final AssertionStdParameterInput parameter) {
    final com.linkedin.assertion.AssertionStdParameter result = new com.linkedin.assertion.AssertionStdParameter();
    result.setType(AssertionStdParameterType.valueOf(parameter.getType().toString()));
    result.setValue(parameter.getValue(), SetMode.IGNORE_NULL);
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.FreshnessAssertionSchedule createFreshnessAssertionSchedule(@Nonnull final FreshnessAssertionScheduleInput schedule) {
    final com.linkedin.assertion.FreshnessAssertionSchedule result = new com.linkedin.assertion.FreshnessAssertionSchedule();
    result.setType(FreshnessAssertionScheduleType.valueOf(schedule.getType().toString()));
    if (schedule.getCron() != null) {
      result.setCron(new FreshnessCronSchedule()
        .setCron(schedule.getCron().getCron())
        .setTimezone(schedule.getCron().getTimezone())
        .setWindowStartOffsetMs(schedule.getCron().getWindowStartOffsetMs(), SetMode.IGNORE_NULL)
      );
    }
    if (schedule.getFixedInterval() != null) {
      result.setFixedInterval(new FixedIntervalSchedule()
          .setMultiple(schedule.getFixedInterval().getMultiple())
          .setUnit(CalendarInterval.valueOf(schedule.getFixedInterval().getUnit().toString()))
      );
    }
    return result;
  }

  @Nonnull
  public static com.linkedin.dataset.DatasetFilter createFreshnessAssertionFilter(@Nonnull final DatasetFilterInput filter) {
    final com.linkedin.dataset.DatasetFilter result = new com.linkedin.dataset.DatasetFilter();
    result.setType(DatasetFilterType.valueOf(filter.getType().toString()));

    if (DatasetFilterType.SQL.equals(result.getType())) {
      if (filter.getSql() != null) {
        result.setSql(filter.getSql());
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. SQL string is required if type Freshness filter type is SQL.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    return result;
  }

  @Nonnull
  public static AssertionActions createAssertionActions(@Nonnull final com.linkedin.datahub.graphql.generated.AssertionActionsInput actions) {
    final AssertionActions result = new AssertionActions();
    result.setOnSuccess(new AssertionActionArray(actions.getOnSuccess().stream()
        .map(action -> new AssertionAction().setType(AssertionActionType.valueOf(action.getType().toString())))
        .collect(Collectors.toList())));
    result.setOnFailure(new AssertionActionArray(actions.getOnFailure().stream()
        .map(action -> new AssertionAction().setType(AssertionActionType.valueOf(action.getType().toString())))
        .collect(Collectors.toList())));
    return result;
  }

  public static boolean isAuthorizedToEditAssertionFromAssertee(final QueryContext context, final Urn asserteeUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        AuthUtils.ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))
    ));
    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        asserteeUrn.getEntityType(),
        asserteeUrn.toString(),
        orPrivilegeGroups);
  }

  public static Urn getAsserteeUrnFromInfo(final AssertionInfo info) {
    switch (info.getType()) {
      case DATASET:
        return info.getDatasetAssertion().getDataset();
      case FRESHNESS:
        return info.getFreshnessAssertion().getEntity();
      default:
        throw new RuntimeException(String.format("Unsupported Assertion Type %s provided", info.getType()));
    }
  }

  private AssertionUtils() { }
}
