package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils.validateInputResources;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ASPECT;

import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.urn.BusinessAttributeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AddBusinessAttributeInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddBusinessAttributeResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final AddBusinessAttributeInput input =
        bindArgument(environment.getArgument("input"), AddBusinessAttributeInput.class);
    final Urn businessAttributeUrn = UrnUtils.getUrn(input.getBusinessAttributeUrn());
    final List<ResourceRefInput> resourceRefInputs = input.getResourceUrn();
    validateBusinessAttribute(context.getOperationContext(), businessAttributeUrn);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            validateInputResources(resourceRefInputs, context);
            addBusinessAttributeToResource(
                context.getOperationContext(),
                businessAttributeUrn,
                resourceRefInputs,
                UrnUtils.getUrn(context.getActorUrn()),
                entityService);
            return true;
          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to add Business Attribute %s to resources %s",
                    businessAttributeUrn, resourceRefInputs));
            throw new RuntimeException(
                String.format(
                    "Failed to add Business Attribute %s to resources %s",
                    businessAttributeUrn, resourceRefInputs),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateBusinessAttribute(
      @Nonnull OperationContext opContext, Urn businessAttributeUrn) {
    if (!entityService.exists(opContext, businessAttributeUrn, true)) {
      throw new IllegalArgumentException(
          String.format("This urn does not exist: %s", businessAttributeUrn));
    }
  }

  private void addBusinessAttributeToResource(
      @Nonnull OperationContext opContext,
      Urn businessAttributeUrn,
      List<ResourceRefInput> resourceRefInputs,
      Urn actorUrn,
      EntityService<?> entityService)
      throws URISyntaxException {
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (ResourceRefInput resourceRefInput : resourceRefInputs) {
      proposals.add(
          buildAddBusinessAttributeToEntityProposal(
              opContext, businessAttributeUrn, resourceRefInput, entityService, actorUrn));
    }
    EntityUtils.ingestChangeProposals(opContext, proposals, entityService, actorUrn, false);
  }

  private MetadataChangeProposal buildAddBusinessAttributeToEntityProposal(
      @Nonnull OperationContext opContext,
      Urn businessAttributeUrn,
      ResourceRefInput resource,
      EntityService entityService,
      Urn actorUrn)
      throws URISyntaxException {
    BusinessAttributes businessAttributes =
        (BusinessAttributes)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                BUSINESS_ATTRIBUTE_ASPECT,
                entityService,
                new BusinessAttributes());
    if (!businessAttributes.hasBusinessAttribute()) {
      businessAttributes.setBusinessAttribute(new BusinessAttributeAssociation());
    }
    BusinessAttributeAssociation businessAttributeAssociation =
        businessAttributes.getBusinessAttribute();
    businessAttributeAssociation.setBusinessAttributeUrn(
        BusinessAttributeUrn.createFromUrn(businessAttributeUrn));
    businessAttributes.setBusinessAttribute(businessAttributeAssociation);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), BUSINESS_ATTRIBUTE_ASPECT, businessAttributes);
  }
}
