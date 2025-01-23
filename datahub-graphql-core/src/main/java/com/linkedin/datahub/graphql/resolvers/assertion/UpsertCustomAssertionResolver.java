package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.PlatformInput;
import com.linkedin.datahub.graphql.generated.UpsertCustomAssertionInput;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
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

    return CompletableFuture.supplyAsync(
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
        });
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

    if (input.getFieldPath() != null) {
      customAssertionInfo.setField(
          SchemaFieldUtils.generateSchemaFieldUrn(entityUrn, input.getFieldPath()));
    }
    return customAssertionInfo;
  }
}
