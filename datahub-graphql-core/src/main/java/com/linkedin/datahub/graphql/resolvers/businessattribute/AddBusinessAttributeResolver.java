package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getFieldInfoFromSchema;

@Slf4j
@RequiredArgsConstructor
public class AddBusinessAttributeResolver implements DataFetcher<CompletableFuture<Boolean>> {
    private final EntityClient _entityClient;
    private final EntityService _entityService;
    @Override
    public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
        final QueryContext context = environment.getContext();
        AddBusinessAttributeInput input = bindArgument(environment.getArgument("input"), AddBusinessAttributeInput.class);
        Urn businessAttributeUrn = UrnUtils.getUrn(input.getBusinessAttributeUrn());
        ResourceRefInput resourceRefInput = input.getResourceUrn();
        //TODO: add authorization check
        if (!_entityClient.exists(businessAttributeUrn, context.getAuthentication())) {
            throw new RuntimeException(String.format("This urn does not exist: %s", businessAttributeUrn));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                validateInputResource(resourceRefInput);
                addBusinessAttribute(businessAttributeUrn, resourceRefInput, context);
                return true;
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to add Business Attribute with urn %s to dataset with urn %s",
                        businessAttributeUrn, resourceRefInput.getResourceUrn()), e);
            }
        });
    }

    private void validateInputResource(ResourceRefInput resource) {
        final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
        LabelUtils.validateResource(resourceUrn, resource.getSubResource(), resource.getSubResourceType(), _entityService);
    }

    private void addBusinessAttribute(Urn businessAttributeUrn, ResourceRefInput resourceRefInput, QueryContext context) throws RemoteInvocationException {
        _entityClient.ingestProposal(
                buildAddBusinessAttributeToSubresourceProposal(businessAttributeUrn, resourceRefInput, context),
                context.getAuthentication()
        );
    }

    private MetadataChangeProposal buildAddBusinessAttributeToSubresourceProposal(Urn businessAttributeUrn, ResourceRefInput resource, QueryContext context) {
        com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
                (com.linkedin.schema.EditableSchemaMetadata) EntityUtils.getAspectFromEntity(
                        resource.getResourceUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                        _entityService, new EditableSchemaMetadata()
                );

        EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

        if (editableFieldInfo == null) {
            throw new IllegalArgumentException(String.format("Subresource %s does not exist in dataset %s",
                    resource.getSubResource(), resource.getResourceUrn()
            ));
        }

        if (editableFieldInfo.hasBusinessAttribute()) {
            throw new RuntimeException(String.format("Schema field has already attached with business attribute"));
        }
        editableFieldInfo.setBusinessAttribute(new BusinessAttributeAssociation());
        addBusinessAttribute(editableFieldInfo.getBusinessAttribute(), businessAttributeUrn, UrnUtils.getUrn(context.getActorUrn()));
        return buildMetadataChangeProposalWithUrn(UrnUtils.getUrn(resource.getResourceUrn()),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME, editableSchemaMetadata);
    }

    private void addBusinessAttribute(BusinessAttributeAssociation businessAttributeAssociation, Urn businessAttributeUrn, Urn actorUrn) {
        businessAttributeAssociation.setDestinationUrn(businessAttributeUrn);
        AuditStamp nowAuditStamp = new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn);
        businessAttributeAssociation.setCreated(nowAuditStamp);
        businessAttributeAssociation.setLastModified(nowAuditStamp);
    }
}
