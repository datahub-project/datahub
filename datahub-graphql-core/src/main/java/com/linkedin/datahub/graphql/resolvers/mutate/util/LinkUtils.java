package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LinkUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private LinkUtils() {}

  public static void addLink(
      @Nonnull OperationContext opContext,
      String linkUrl,
      String linkLabel,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    InstitutionalMemory institutionalMemoryAspect =
        (InstitutionalMemory)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
                entityService,
                new InstitutionalMemory());
    addLink(institutionalMemoryAspect, linkUrl, linkLabel, actor);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        institutionalMemoryAspect,
        actor,
        entityService);
  }

  public static void removeLink(
      @Nonnull OperationContext opContext,
      String linkUrl,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    InstitutionalMemory institutionalMemoryAspect =
        (InstitutionalMemory)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
                entityService,
                new InstitutionalMemory());
    removeLink(institutionalMemoryAspect, linkUrl);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        institutionalMemoryAspect,
        actor,
        entityService);
  }

  private static void addLink(
      InstitutionalMemory institutionalMemoryAspect, String linkUrl, String linkLabel, Urn actor) {
    if (!institutionalMemoryAspect.hasElements()) {
      institutionalMemoryAspect.setElements(new InstitutionalMemoryMetadataArray());
    }

    InstitutionalMemoryMetadataArray linksArray = institutionalMemoryAspect.getElements();

    // if link exists, do not add it again
    if (linksArray.stream().anyMatch(link -> link.getUrl().toString().equals(linkUrl))) {
      return;
    }

    InstitutionalMemoryMetadata newLink = new InstitutionalMemoryMetadata();
    newLink.setUrl(new Url(linkUrl));
    newLink.setCreateStamp(EntityUtils.getAuditStamp(actor));
    newLink.setDescription(linkLabel); // We no longer support, this is really a label.
    linksArray.add(newLink);
  }

  private static void removeLink(InstitutionalMemory institutionalMemoryAspect, String linkUrl) {
    if (!institutionalMemoryAspect.hasElements()) {
      institutionalMemoryAspect.setElements(new InstitutionalMemoryMetadataArray());
    }

    InstitutionalMemoryMetadataArray elementsArray = institutionalMemoryAspect.getElements();
    elementsArray.removeIf(link -> link.getUrl().toString().equals(linkUrl));
  }

  public static boolean isAuthorizedToUpdateLinks(@Nonnull QueryContext context, Urn resourceUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOC_LINKS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, resourceUrn.getEntityType(), resourceUrn.toString(), orPrivilegeGroups);
  }

  public static Boolean validateAddRemoveInput(
      @Nonnull OperationContext opContext,
      String linkUrl,
      Urn resourceUrn,
      EntityService<?> entityService) {

    try {
      new Url(linkUrl);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change institutional memory for resource %s. Expected a corp group urn.",
              resourceUrn));
    }

    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change institutional memory for resource %s. Resource does not exist.",
              resourceUrn));
    }

    return true;
  }
}
