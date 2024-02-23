package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.linkedin.common.AccessRequest;
import com.linkedin.common.AccessRequestArray;
import com.linkedin.common.AccessRequests;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.List;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

@Slf4j
public class AccessRequestUtils {

    public static boolean isAuthorizedToRemoveAccessRequest(
            @Nonnull QueryContext context, Urn resourceUrn, Urn requesterUrn, Long requestedAt, EntityService<?> entityService) {

        AccessRequests accessRequestAspect = getAccessRequests(resourceUrn, entityService);

        return accessRequestAspect.getRequests().stream()
                .filter(accessRequest ->
                        accessRequest.getCreated().getTime().equals(requestedAt) &&
                                accessRequest.getCreated().getActor().equals(requesterUrn))
                .anyMatch(accessRequest -> isActorTheCreator(context, accessRequest));
    }

    public static AccessRequests getAccessRequests(Urn resourceUrn, EntityService<?> entityService) {
        AccessRequests accessRequests = (AccessRequests) EntityUtils.getAspectFromEntity(
                resourceUrn.toString(),
                Constants.ACCESS_REQUESTS_ASPECT_NAME,
                entityService,
                new AccessRequests());

        if (accessRequests != null && !accessRequests.hasRequests()) {
            accessRequests.setRequests(new AccessRequestArray());
        }
        return accessRequests;
    }

    public static void removeAccessRequest(Urn resourceUrn, Urn requesterUrn, Long requestedAt, Urn actor, EntityService<?> entityService) {
        AccessRequests accessRequestAspect = getAccessRequests(resourceUrn, entityService);

        accessRequestAspect.getRequests().removeIf(accessRequest ->
                accessRequest.getCreated().getTime().equals(requestedAt) &&
                        accessRequest.getCreated().getActor().equals(requesterUrn));
        persistAspect(
            resourceUrn,
            Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
            accessRequestAspect,
            actor,
            entityService);
    }


    private static boolean isActorTheCreator(QueryContext context, AccessRequest accessRequest) {
        return accessRequest.getCreated().getActor().toString().equals(context.getActorUrn());
    }


    public static void addAccessRequestToEntity(Urn actor, List<StringMapEntryInput> additionalMetadataInput, Urn targetUrn, EntityService<?> entityService) {
        AccessRequest accessRequest = generateAccessRequest(actor, additionalMetadataInput);

        AccessRequests currentAccessRequests = getAccessRequests(targetUrn, entityService);
        AccessRequestArray accessRequestArray = currentAccessRequests.getRequests();
        accessRequestArray.add(accessRequest);

        persistAspect(
                targetUrn,
                Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
                currentAccessRequests,
                actor,
                entityService);
    }

    private static AccessRequest generateAccessRequest(Urn actor, List<StringMapEntryInput> additionalMetadataInput) {
        AuditStamp created = EntityUtils.getAuditStamp(actor);

        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setCreated(created);

        StringMap additionalMetadata = new StringMap();

        for (StringMapEntryInput element : additionalMetadataInput) {
            additionalMetadata.put(element.getKey(), element.getValue());
        }
        accessRequest.setAdditionalMetadata(additionalMetadata);
        return accessRequest;
    }


    public static Boolean validateRemoveInput(
            Urn resourceUrn, Urn requesterUrn, Long requestedAt, EntityService<?> entityService) {

        boolean accessRequestExists = getAccessRequests(resourceUrn, entityService)
                .getRequests().stream().anyMatch(accessRequest ->
                        accessRequest.getCreated().getTime().equals(requestedAt) &&
                                accessRequest.getCreated().getActor().equals(requesterUrn));

        if (!accessRequestExists) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to remove access request for resource %s. Access Request does not exist.",
                            resourceUrn));
        }

        if (!entityService.exists(resourceUrn, true)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to remove access request for resource %s. Resource does not exist.",
                            resourceUrn));
        }

        return true;
    }
}
