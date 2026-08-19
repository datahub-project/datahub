package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.PlatformInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpsertCustomAssertionInput;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertCustomAssertionResolver implements DataFetcher<CompletableFuture<Assertion>> {

  private final AssertionService _assertionService;

  public UpsertCustomAssertionResolver(@Nonnull final AssertionService assertionService) {
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<Assertion> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String maybeAssertionUrn = environment.getArgument("urn");
    final UpsertCustomAssertionInput input =
        bindArgument(environment.getArgument("input"), UpsertCustomAssertionInput.class);

    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    final Urn assertionUrn;

    if (maybeAssertionUrn == null) {
      assertionUrn = _assertionService.generateAssertionUrn();
    } else {
      assertionUrn = UrnUtils.getUrn(maybeAssertionUrn);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Check whether the current user is allowed to update the assertion.
          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, entityUrn)) {
            _assertionService.upsertCustomAssertion(
                context.getOperationContext(),
                assertionUrn,
                entityUrn,
                input.getDescription(),
                input.getExternalUrl(),
                mapAssertionPlatform(input.getPlatform()),
                createCustomAssertionInfo(input, entityUrn));

            return AssertionMapper.map(
                context,
                _assertionService.getAssertionEntityResponse(
                    context.getOperationContext(), assertionUrn));
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @SneakyThrows
  private DataPlatformInstance mapAssertionPlatform(PlatformInput platformInput) {
    DataPlatformInstance platform = new DataPlatformInstance();
    if (platformInput.getUrn() != null) {
      platform.setPlatform(Urn.createFromString(platformInput.getUrn()));
    } else if (platformInput.getName() != null) {
      platform.setPlatform(
          EntityKeyUtils.convertEntityKeyToUrn(
              new DataPlatformKey().setPlatformName(platformInput.getName()),
              DATA_PLATFORM_ENTITY_NAME));
    } else {
      throw new IllegalArgumentException(
          "Failed to upsert Custom Assertion. Platform Name or Platform Urn must be specified.");
    }

    return platform;
  }

  private CustomAssertionInfo createCustomAssertionInfo(
      UpsertCustomAssertionInput input, Urn entityUrn) {
    CustomAssertionInfo customAssertionInfo = new CustomAssertionInfo();
    customAssertionInfo.setType(input.getType());
    customAssertionInfo.setEntity(entityUrn);
    customAssertionInfo.setLogic(input.getLogic(), SetMode.IGNORE_NULL);

    List<String> fieldPaths = input.getFieldPaths();
    if (fieldPaths != null && !fieldPaths.isEmpty()) {
      UrnArray fieldUrns = new UrnArray();
      for (String fieldPath : fieldPaths) {
        if (fieldPath == null || fieldPath.isBlank()) {
          throw new IllegalArgumentException(
              "Failed to upsert Custom Assertion. fieldPaths must not contain blank entries;"
                  + " omit fieldPaths for dataset-level assertions.");
        }
        fieldUrns.add(SchemaFieldUtils.generateSchemaFieldUrn(entityUrn, fieldPath));
      }
      customAssertionInfo.setFields(fieldUrns);
      // Keep singular field populated for backward compatibility
      customAssertionInfo.setField(fieldUrns.get(0));
    } else if (input.getFieldPath() != null) {
      if (input.getFieldPath().isBlank()) {
        throw new IllegalArgumentException(
            "Failed to upsert Custom Assertion. fieldPath must not be blank when provided;"
                + " omit fieldPath for dataset-level assertions.");
      }
      Urn fieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(entityUrn, input.getFieldPath());
      customAssertionInfo.setField(fieldUrn);
      customAssertionInfo.setFields(new UrnArray(fieldUrn));
    }

    if (input.getScope() != null) {
      customAssertionInfo.setScope(DatasetAssertionScope.valueOf(input.getScope().name()));
    }
    if (input.getAggregation() != null) {
      customAssertionInfo.setAggregation(
          AssertionStdAggregation.valueOf(input.getAggregation().name()));
    }
    if (input.getOperator() != null) {
      customAssertionInfo.setOperator(AssertionStdOperator.valueOf(input.getOperator().name()));
    }
    if (input.getParameters() != null) {
      customAssertionInfo.setParameters(mapParameters(input.getParameters()));
    }
    customAssertionInfo.setNativeType(input.getNativeType(), SetMode.IGNORE_NULL);
    if (input.getNativeParameters() != null && !input.getNativeParameters().isEmpty()) {
      customAssertionInfo.setNativeParameters(mapNativeParameters(input.getNativeParameters()));
    }
    return customAssertionInfo;
  }

  @Nullable
  private AssertionStdParameters mapParameters(
      @Nonnull AssertionStdParametersInput parametersInput) {
    AssertionStdParameters parameters = new AssertionStdParameters();
    if (parametersInput.getValue() != null) {
      parameters.setValue(mapParameter(parametersInput.getValue()));
    }
    if (parametersInput.getMinValue() != null) {
      parameters.setMinValue(mapParameter(parametersInput.getMinValue()));
    }
    if (parametersInput.getMaxValue() != null) {
      parameters.setMaxValue(mapParameter(parametersInput.getMaxValue()));
    }
    return parameters;
  }

  private AssertionStdParameter mapParameter(@Nonnull AssertionStdParameterInput parameterInput) {
    return new AssertionStdParameter()
        .setType(AssertionStdParameterType.valueOf(parameterInput.getType().name()))
        .setValue(parameterInput.getValue());
  }

  private StringMap mapNativeParameters(@Nonnull List<StringMapEntryInput> entries) {
    StringMap map = new StringMap();
    for (StringMapEntryInput entry : entries) {
      if (entry.getKey() != null && entry.getValue() != null) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }
}
