package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActionType;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.RunAssertionResult;
import com.linkedin.datahub.graphql.generated.RunAssertionsResult;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpecInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.TestAssertionInput;
import com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils;
import com.linkedin.datahub.graphql.types.dataset.mappers.AssertionRunEventMapper;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class AssertionUtils {

  public static final int MAX_ASSERTIONS_TO_RUN_ON_DEMAND = 20;

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
        context, asserteeUrn.getEntityType(), asserteeUrn.toString(), orPrivilegeGroups);
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
      case CUSTOM:
        return info.getCustomAssertion().getEntity();
      default:
        throw new RuntimeException(
            String.format("Unsupported Assertion Type %s provided", info.getType()));
    }
  }

  public static String getAsserteeUrnFromTestInput(final TestAssertionInput input) {
    switch (input.getType()) {
      case FRESHNESS:
        if (input.getFreshnessTestInput() != null) {
          return input.getFreshnessTestInput().getEntityUrn();
        }
        throw new RuntimeException("Freshness Test Input is required for Freshness Assertion");
      case VOLUME:
        if (input.getVolumeTestInput() != null) {
          return input.getVolumeTestInput().getEntityUrn();
        }
        throw new RuntimeException("Volume Test Input is required for Volume Assertion");
      case SQL:
        if (input.getSqlTestInput() != null) {
          return input.getSqlTestInput().getEntityUrn();
        }
        throw new RuntimeException("SQL Test Input is required for SQL Assertion");
      case FIELD:
        if (input.getFieldTestInput() != null) {
          return input.getFieldTestInput().getEntityUrn();
        }
        throw new RuntimeException("Field Test Input is required for Field Assertion");
      case DATA_SCHEMA:
        if (input.getSchemaTestInput() != null) {
          return input.getSchemaTestInput().getEntityUrn();
        }
        throw new RuntimeException("Schema Test Input is required for Schema Assertion");
      default:
        throw new InvalidAssertionTypeException(
            String.format("Unsupported Assertion Type %s provided", input.getType()));
    }
  }

  public static void validateAssertionSource(
      @Nonnull final Urn assertionUrn, @Nonnull final AssertionInfo info) {
    final AssertionSource assertionSource = info.getSource(GetMode.NULL);

    if (assertionSource == null
        || (!AssertionSourceType.NATIVE.equals(assertionSource.getType())
            && !AssertionSourceType.INFERRED.equals(assertionSource.getType()))) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not a valid DataHub assertion.",
              assertionUrn));
    }
  }

  public static RunAssertionsResult extractRunResults(
      @Nonnull final QueryContext context,
      @Nonnull final List<Urn> assertionUrns,
      @Nonnull final Map<Urn, AssertionResult> results) {
    /* Map each result back */
    RunAssertionsResult result = new RunAssertionsResult();

    int passingCount = 0;
    int failingCount = 0;
    int errorCount = 0;
    final List<RunAssertionResult> runResults = new ArrayList<>();

    for (Urn assertionUrn : assertionUrns) {
      final AssertionResult assertionResult = results.get(assertionUrn);
      if (assertionResult == null) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to run Assertion. Monitors service returns 'null' result for assertion with urn %s",
                assertionUrn));
      }
      RunAssertionResult runResult = new RunAssertionResult();
      runResult.setResult(AssertionRunEventMapper.mapResult(context, assertionResult));

      Assertion unresolvedAssertion = new Assertion();
      unresolvedAssertion.setUrn(assertionUrn.toString());
      runResult.setAssertion(unresolvedAssertion);

      runResults.add(runResult);

      switch (assertionResult.getType()) {
        case SUCCESS:
          passingCount++;
          break;
        case FAILURE:
          failingCount++;
          break;
        case ERROR:
          errorCount++;
          break;
      }
    }

    result.setResults(runResults);
    result.setPassingCount(passingCount);
    result.setFailingCount(failingCount);
    result.setErrorCount(errorCount);

    return result;
  }

  public static boolean isAuthorizedToRunAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final AssertionType type,
      @Nonnull final QueryContext context) {
    // We must be able to both create assertions + monitors.
    if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {
      // Check whether we are allowed to test sensitive monitor types (Custom SQL).
      if (AssertionType.SQL.equals(type)) {
        return MonitorUtils.isAuthorizedToUpdateSqlAssertionMonitors(asserteeUrn, context);
      }
      // User is authorized.
      return MonitorUtils.isAuthorizedToUpdateEntityMonitors(asserteeUrn, context);
    }
    // Unauthorized
    return false;
  }

  @Nonnull
  public static Map<String, String> extractStringMapEntryInputList(
      DataFetchingEnvironment environment) {
    final List<Object> parameterObjs =
        environment.getArgumentOrDefault("parameters", Collections.emptyList());
    final List<StringMapEntryInput> parameters =
        parameterObjs.stream()
            .map(obj -> bindArgument(obj, StringMapEntryInput.class))
            .collect(Collectors.toList());
    return parameters.stream()
        .collect(Collectors.toMap(StringMapEntryInput::getKey, StringMapEntryInput::getValue));
  }

  private AssertionUtils() {}
}
