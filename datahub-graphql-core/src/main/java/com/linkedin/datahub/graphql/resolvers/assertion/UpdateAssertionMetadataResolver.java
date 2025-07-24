package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionNote;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.UpdateAssertionMetadataInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateAssertionMetadataResolver implements DataFetcher<CompletableFuture<Assertion>> {

  private final AssertionService _assertionService;
  private final LongSupplier _timeSupplier;

  public UpdateAssertionMetadataResolver(
      @Nonnull final AssertionService assertionService, @Nonnull final LongSupplier timeSupplier) {
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
    _timeSupplier = Objects.requireNonNull(timeSupplier, "timeSupplier is required");
  }

  public UpdateAssertionMetadataResolver(@Nonnull final AssertionService assertionService) {
    this(assertionService, System::currentTimeMillis);
  }

  @Override
  public CompletableFuture<Assertion> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final Urn assertionUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final UpdateAssertionMetadataInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpdateAssertionMetadataInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Check whether the current user is allowed to update the assertion.
          final AssertionInfo info =
              _assertionService.getAssertionInfo(context.getOperationContext(), assertionUrn);

          if (info == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Failed to update Assertion. Assertion with urn %s does not exist.",
                    assertionUrn));
          }

          final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);

          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {

            // First update the existing assertion.
            _assertionService.updateAssertionMetadata(
                context.getOperationContext(),
                assertionUrn,
                input.getActions() != null
                    ? AssertionUtils.createAssertionActions(input.getActions())
                    : null,
                input.getDescription(),
                input.getNote() != null
                    ? new AssertionNote()
                        .setContent(input.getNote())
                        .setLastModified(
                            new AuditStamp()
                                .setActor(UrnUtils.getUrn(context.getActorUrn()))
                                .setTime(_timeSupplier.getAsLong()))
                    : null);

            // Then, return the new assertion
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
}
