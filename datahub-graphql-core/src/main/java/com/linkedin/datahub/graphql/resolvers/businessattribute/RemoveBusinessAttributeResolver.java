package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.businessattribute.BusinessAttributeAuthorizationUtils.isAuthorizeToUpdateDataset;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getFieldInfoFromSchema;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AddBusinessAttributeInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveBusinessAttributeResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    AddBusinessAttributeInput input =
        bindArgument(environment.getArgument("input"), AddBusinessAttributeInput.class);
    Urn businessAttributeUrn = UrnUtils.getUrn(input.getBusinessAttributeUrn());
    ResourceRefInput resourceRefInput = input.getResourceUrn();
    if (!isAuthorizeToUpdateDataset(
        context, Urn.createFromString(resourceRefInput.getResourceUrn()))) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!businessAttributeUrn.getEntityType().equals("businessAttribute")) {
              log.error(
                  "Failed to remove {}. It is not a business attribute urn.",
                  businessAttributeUrn.toString());
              return false;
            }

            validateInputResource(resourceRefInput, context);

            removeBusinessAttribute(resourceRefInput, context);

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to remove Business Attribute with urn %s to dataset with urn %s",
                    businessAttributeUrn, resourceRefInput.getResourceUrn()),
                e);
          }
        });
  }

  private void validateInputResource(ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
    LabelUtils.validateResource(
        resourceUrn, resource.getSubResource(), resource.getSubResourceType(), _entityService);
  }

  private void removeBusinessAttribute(ResourceRefInput resourceRefInput, QueryContext context)
      throws RemoteInvocationException {
    _entityClient.ingestProposal(
        buildRemoveBusinessAttributeToSubresourceProposal(resourceRefInput),
        context.getAuthentication());
  }

  private MetadataChangeProposal buildRemoveBusinessAttributeToSubresourceProposal(
      ResourceRefInput resource) {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata)
            EntityUtils.getAspectFromEntity(
                resource.getResourceUrn(),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                _entityService,
                new EditableSchemaMetadata());

    EditableSchemaFieldInfo editableFieldInfo =
        getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (editableFieldInfo == null) {
      throw new IllegalArgumentException(
          String.format(
              "Subresource %s does not exist in dataset %s",
              resource.getSubResource(), resource.getResourceUrn()));
    }

    if (!editableFieldInfo.hasBusinessAttribute()) {
      throw new RuntimeException(
          String.format("Schema field has not attached with business attribute"));
    }
    editableFieldInfo.removeBusinessAttribute();
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }
}
