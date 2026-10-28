package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.REPOSITORY_ENTITY_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.repository.RepositoryLineage;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Restricts {@code repositoryLineage} edges on dataJob and dataFlow to repository destinations.
 *
 * <p>The aspect's relationship annotations allow dataset destinations because a repository's own
 * code reads and writes datasets. On a pipeline or task the only meaningful edge is to the
 * repository holding its source code; dataset edges belong in {@code dataJobInputOutput}. The
 * annotations live on the shared aspect, so the restriction is enforced here. Runs at pre-commit so
 * a PATCH is checked against the merged result.
 */
@Setter
@Getter
@Accessors(chain = true)
public class RepositoryLineageDestinationValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (ChangeMCP item : changeMCPs) {
      RepositoryLineage lineage = item.getAspect(RepositoryLineage.class);
      if (lineage == null) {
        continue;
      }
      checkEdges(item, "inputEdges", lineage.getInputEdges(), exceptions);
      checkEdges(item, "outputEdges", lineage.getOutputEdges(), exceptions);
    }
    return exceptions.streamAllExceptions();
  }

  private static void checkEdges(
      @Nonnull BatchItem item,
      @Nonnull String fieldName,
      @Nullable EdgeArray edges,
      @Nonnull ValidationExceptionCollection exceptions) {
    if (edges == null) {
      return;
    }
    for (Edge edge : edges) {
      Urn destination = edge.getDestinationUrn();
      if (destination != null && !REPOSITORY_ENTITY_NAME.equals(destination.getEntityType())) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "repositoryLineage.%s on a %s may only point at a repository, got %s",
                    fieldName, item.getUrn().getEntityType(), destination)));
      }
    }
  }
}
