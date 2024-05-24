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
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpecInput;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class AssertionUtils {

  @Nonnull
  public static com.linkedin.assertion.AssertionStdParameters createDatasetAssertionParameters(
      @Nonnull final AssertionStdParametersInput parameters) {
    final com.linkedin.assertion.AssertionStdParameters result =
        new com.linkedin.assertion.AssertionStdParameters();
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
  public static com.linkedin.assertion.AssertionStdParameter createDatasetAssertionParameter(
      @Nonnull final AssertionStdParameterInput parameter) {
    final com.linkedin.assertion.AssertionStdParameter result =
        new com.linkedin.assertion.AssertionStdParameter();
    result.setType(AssertionStdParameterType.valueOf(parameter.getType().toString()));
    result.setValue(parameter.getValue(), SetMode.IGNORE_NULL);
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.AssertionStdOperator createAssertionStdOperator(
      @Nonnull final AssertionStdOperator operator) {
    return com.linkedin.assertion.AssertionStdOperator.valueOf(operator.toString());
  }

  @Nonnull
  public static com.linkedin.schema.SchemaFieldSpec createSchemaFieldSpec(
      @Nonnull final SchemaFieldSpecInput input) {
    final com.linkedin.schema.SchemaFieldSpec result = new com.linkedin.schema.SchemaFieldSpec();
    result.setType(input.getType());
    result.setNativeType(input.getNativeType());
    result.setPath(input.getPath());
    return result;
  }

  @Nonnull
  public static com.linkedin.dataset.DatasetFilter createAssertionFilter(
      @Nonnull final DatasetFilterInput filter) {
    final com.linkedin.dataset.DatasetFilter result = new com.linkedin.dataset.DatasetFilter();
    result.setType(DatasetFilterType.valueOf(filter.getType().toString()));

    if (DatasetFilterType.SQL.equals(result.getType())) {
      if (filter.getSql() != null) {
        result.setSql(filter.getSql());
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. SQL string is required if type DatasetFilter type is SQL.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    return result;
  }

  @Nonnull
  public static AssertionActions createAssertionActions(
      @Nonnull final com.linkedin.datahub.graphql.generated.AssertionActionsInput actions) {
    final AssertionActions result = new AssertionActions();
    result.setOnSuccess(
        new AssertionActionArray(
            actions.getOnSuccess().stream()
                .map(
                    action ->
                        new AssertionAction()
                            .setType(AssertionActionType.valueOf(action.getType().toString())))
                .collect(Collectors.toList())));
    result.setOnFailure(
        new AssertionActionArray(
            actions.getOnFailure().stream()
                .map(
                    action ->
                        new AssertionAction()
                            .setType(AssertionActionType.valueOf(action.getType().toString())))
                .collect(Collectors.toList())));
    return result;
  }

  public static boolean isAuthorizedToEditAssertionFromAssertee(
      final QueryContext context, final Urn asserteeUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                AuthorizationUtils.ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))));
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
      case VOLUME:
        return info.getVolumeAssertion().getEntity();
      case SQL:
        return info.getSqlAssertion().getEntity();
      case FIELD:
        return info.getFieldAssertion().getEntity();
      case DATA_SCHEMA:
        return info.getSchemaAssertion().getEntity();
      default:
        throw new RuntimeException(
            String.format("Unsupported Assertion Type %s provided", info.getType()));
    }
  }

  private AssertionUtils() {}
}
