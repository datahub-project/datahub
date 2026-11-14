package com.linkedin.metadata.aspect.validation;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Requires the discoveredDataSource entity privileges (Manage Ingestion, or the matching entity
 * privilege) for any write to its aspects, regardless of which API the write arrives through.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DiscoveredDataSourceAuthorizationValidator
    extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : items) {
      ApiOperation operation = toApiOperation(item.getChangeType());
      if (!AuthUtil.isAuthorizedEntityUrns(session, operation, List.of(item.getUrn()))) {
        failures.add(
            authFailure(
                item,
                String.format(
                    "Unauthorized to %s discovered data source: %s",
                    operation.name().toLowerCase(), item.getUrn())));
      }
    }
    return failures;
  }

  private static ApiOperation toApiOperation(@Nonnull ChangeType changeType) {
    switch (changeType) {
      case CREATE:
      case CREATE_ENTITY:
        return ApiOperation.CREATE;
      case DELETE:
        return ApiOperation.DELETE;
      default:
        return ApiOperation.UPDATE;
    }
  }
}
