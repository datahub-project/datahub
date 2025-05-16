package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.InferredColumnDescriptions;
import com.linkedin.datahub.graphql.generated.InferredDocumentation;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.integrations.model.SuggestedDescription;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class InferDocumentationResolver
    implements DataFetcher<CompletableFuture<InferredDocumentation>> {
  private final IntegrationsService _integrationsService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<InferredDocumentation> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn targetUrn =
        Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));
    final Boolean saveResult =
        bindArgument(environment.getArgumentOrDefault("saveResult", false), Boolean.class);
    if (saveResult && !DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return _integrationsService
        .inferDocumentation(targetUrn)
        .thenCompose(
            inferredDocumentation ->
                GraphQLConcurrencyUtils.supplyAsync(
                    () -> {
                      if (saveResult) {
                        saveInferredDocumentation(
                            context.getOperationContext(), targetUrn, inferredDocumentation);
                      }
                      return mapInferredDocumentation(inferredDocumentation);
                    },
                    this.getClass().getSimpleName(),
                    "get"));
  }

  private void saveInferredDocumentation(
      @Nonnull final OperationContext operationContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final SuggestedDescription inferredDocumentation) {
    final List<MetadataChangeProposal> metadataChangeProposals = new ArrayList<>();
    if (!inferredDocumentation.getEntityDescription().isEmpty()) {
      metadataChangeProposals.add(
          getInferredDocumentationMCPForEntity(
              operationContext, entityUrn, inferredDocumentation.getEntityDescription()));
    }
    if (!inferredDocumentation.getColumnDescriptions().isEmpty()) {
      inferredDocumentation
          .getColumnDescriptions()
          .forEach(
              (key, value) -> {
                final Urn schemaUrn = SchemaFieldUtils.generateSchemaFieldUrn(entityUrn, key);
                metadataChangeProposals.add(
                    getInferredDocumentationMCPForEntity(operationContext, schemaUrn, value));
              });
    }

    try {
      // TODO: switch to patch
      _entityClient.batchIngestProposals(operationContext, metadataChangeProposals, false);
    } catch (Exception e) {
      log.error(
          String.format("Failed to save asset documentation for entity with urn %s", entityUrn), e);
    }
  }

  private MetadataChangeProposal getInferredDocumentationMCPForEntity(
      @Nonnull final OperationContext operationContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String description) {
    DataMap existingDocData = null;
    try {
      final EntityResponse response =
          _entityClient.getV2(
              operationContext,
              entityUrn.getEntityType(),
              entityUrn,
              Set.of(Constants.DOCUMENTATION_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.DOCUMENTATION_ASPECT_NAME)) {
        existingDocData =
            response.getAspects().get(Constants.DOCUMENTATION_ASPECT_NAME).getValue().data();
      }
    } catch (Exception e) {
      log.error(String.format("Failed to get documentation for entity %s", entityUrn), e);
      return null;
    }
    final Documentation documentation =
        existingDocData == null ? new Documentation() : new Documentation(existingDocData);

    // If we already have docs, filter out the AI ones as we'll be adding a new one
    final DocumentationAssociationArray associations =
        !documentation.hasDocumentations()
            ? new DocumentationAssociationArray()
            : new DocumentationAssociationArray(
                documentation.getDocumentations().stream()
                    .filter(
                        el ->
                            !(el.hasAttribution()
                                && el.getAttribution().hasSourceDetail()
                                && Objects.equals(
                                    el.getAttribution().getSourceDetail().get("inferred"), "true")))
                    .collect(Collectors.toSet()));

    // Add AI gen docs
    final MetadataAttribution attribution =
        new MetadataAttribution()
            .setTime(operationContext.getAuditStamp().getTime())
            .setActor(operationContext.getAuditStamp().getActor())
            .setSourceDetail(new StringMap(Map.of("inferred", "true")));

    associations.add(
        new DocumentationAssociation().setDocumentation(description).setAttribution(attribution));

    // Update top aspect
    documentation.setLastModified(operationContext.getAuditStamp()).setDocumentations(associations);
    return AspectUtils.buildMetadataChangeProposal(
        entityUrn, Constants.DOCUMENTATION_ASPECT_NAME, documentation);
  }

  private InferredDocumentation mapInferredDocumentation(
      @Nonnull final SuggestedDescription inferredDocumentation) {
    final InferredDocumentation result = new InferredDocumentation();
    if (Objects.nonNull(inferredDocumentation.getEntityDescription())) {
      result.setEntityDescription(inferredDocumentation.getEntityDescription());
    }
    if (Objects.nonNull(inferredDocumentation.getColumnDescriptions())) {
      final InferredColumnDescriptions inferredColumnDescriptions =
          new InferredColumnDescriptions();
      inferredColumnDescriptions.setJsonBlob(
          JSON.serialize(inferredDocumentation.getColumnDescriptions()));
      result.setColumnDescriptions(inferredColumnDescriptions);
    }
    return result;
  }
}
