package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.InstitutionalMemoryMetadataSettings;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.LinkSettingsInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
      @Nullable LinkSettingsInput settingsInput,
      EntityService<?> entityService) {
    InstitutionalMemory institutionalMemoryAspect =
        (InstitutionalMemory)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
                entityService,
                new InstitutionalMemory());
    addLink(institutionalMemoryAspect, linkUrl, linkLabel, actor, settingsInput);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        institutionalMemoryAspect,
        actor,
        entityService);
  }

  public static void updateLink(
      @Nonnull OperationContext opContext,
      String currentLinkUrl,
      String currentLinkLabel,
      String newLinkUrl,
      String newLinkLabel,
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

    updateLink(
        institutionalMemoryAspect,
        currentLinkUrl,
        currentLinkLabel,
        newLinkUrl,
        newLinkLabel,
        actor);
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
      String label,
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
    removeLink(institutionalMemoryAspect, linkUrl, label);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        institutionalMemoryAspect,
        actor,
        entityService);
  }

  private static void addLink(
      InstitutionalMemory institutionalMemoryAspect,
      String linkUrl,
      String linkLabel,
      Urn actor,
      @Nullable LinkSettingsInput settingsInput) {
    if (!institutionalMemoryAspect.hasElements()) {
      institutionalMemoryAspect.setElements(new InstitutionalMemoryMetadataArray());
    }

    InstitutionalMemoryMetadataArray linksArray = institutionalMemoryAspect.getElements();

    InstitutionalMemoryMetadata newLink = new InstitutionalMemoryMetadata();
    newLink.setUrl(new Url(linkUrl));
    newLink.setCreateStamp(EntityUtils.getAuditStamp(actor));
    newLink.setDescription(linkLabel); // We no longer support, this is really a label.
    if (settingsInput != null) {
      newLink.setSettings(mapSettings(settingsInput));
    }

    // if link exists, do not add it again
    if (hasDuplicates(linksArray, newLink)) {
      return;
    }

    linksArray.add(newLink);
  }

  private static void removeLink(
      InstitutionalMemory institutionalMemoryAspect, String linkUrl, String label) {
    if (!institutionalMemoryAspect.hasElements()) {
      institutionalMemoryAspect.setElements(new InstitutionalMemoryMetadataArray());
    }

    InstitutionalMemoryMetadataArray elementsArray = institutionalMemoryAspect.getElements();

    // when label is passed, it's needed to remove link with the same url and label
    if (label != null) {
      elementsArray.removeIf(getPredicate(linkUrl, label));
    } else {
      elementsArray.removeIf(link -> link.getUrl().toString().equals(linkUrl));
    }
  }

  private static void updateLink(
      InstitutionalMemory institutionalMemoryAspect,
      String currentLinkUrl,
      String currentLinkLabel,
      String newLinkUrl,
      String newLinkLabel,
      Urn actor) {
    if (!institutionalMemoryAspect.hasElements()) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update the link '%s' with label '%s'. The link does not exist",
              currentLinkUrl, currentLinkLabel));
    }

    InstitutionalMemoryMetadataArray elementsArray = institutionalMemoryAspect.getElements();

    Optional<InstitutionalMemoryMetadata> optionalLinkToReplace =
        elementsArray.stream().filter(getPredicate(currentLinkUrl, currentLinkLabel)).findFirst();
    if (optionalLinkToReplace.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update the link '%s' with label '%s'. The link does not exist",
              currentLinkUrl, currentLinkLabel));
    }
    InstitutionalMemoryMetadata linkToReplace = optionalLinkToReplace.get();

    InstitutionalMemoryMetadata updatedLink = new InstitutionalMemoryMetadata();
    updatedLink.setUrl(new Url(newLinkUrl));
    updatedLink.setDescription(newLinkLabel);
    updatedLink.setCreateStamp(linkToReplace.getCreateStamp());
    updatedLink.setUpdateStamp(EntityUtils.getAuditStamp(actor));

    InstitutionalMemoryMetadataArray linksWithoutReplacingOne =
        new InstitutionalMemoryMetadataArray();
    linksWithoutReplacingOne.addAll(
        elementsArray.stream().filter(link -> !link.equals(linkToReplace)).toList());

    if (hasDuplicates(linksWithoutReplacingOne, updatedLink)) {
      throw new IllegalArgumentException(
          String.format("The link '%s' with label '%s' already exists", newLinkUrl, newLinkLabel));
    }

    elementsArray.set(elementsArray.indexOf(linkToReplace), updatedLink);
  }

  private static Predicate<InstitutionalMemoryMetadata> getPredicate(
      String linkUrl, String linkLabel) {
    return (link) ->
        link.getUrl().toString().equals(linkUrl) & link.getDescription().equals((linkLabel));
  }

  private static boolean hasDuplicates(
      InstitutionalMemoryMetadataArray linksArray, InstitutionalMemoryMetadata linkToValidate) {
    String url = linkToValidate.getUrl().toString();
    String label = linkToValidate.getDescription();

    return linksArray.stream().anyMatch(getPredicate(url, label));
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

  public static void validateAddRemoveInput(
      @Nonnull OperationContext opContext,
      String linkUrl,
      Urn resourceUrn,
      EntityService<?> entityService) {
    validateUrl(linkUrl, resourceUrn);
    validateResourceUrn(opContext, resourceUrn, entityService);
  }

  public static void validateUpdateInput(
      @Nonnull OperationContext opContext,
      String currentLinkUrl,
      String linkUrl,
      Urn resourceUrn,
      EntityService<?> entityService) {
    validateUrl(currentLinkUrl, resourceUrn);
    validateUrl(linkUrl, resourceUrn);
    validateResourceUrn(opContext, resourceUrn, entityService);
  }

  private static void validateUrl(String url, Urn resourceUrn) {
    try {
      new Url(url);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change institutional memory for resource %s. Expected an url.",
              resourceUrn));
    }
  }

  private static void validateResourceUrn(
      @Nonnull OperationContext opContext, Urn resourceUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change institutional memory for resource %s. Resource does not exist.",
              resourceUrn));
    }
  }

  private static InstitutionalMemoryMetadataSettings mapSettings(LinkSettingsInput settingsInput) {
    InstitutionalMemoryMetadataSettings settings = new InstitutionalMemoryMetadataSettings();
    if (settingsInput.getShowInAssetPreview() != null) {
      settings.setShowInAssetPreview(settingsInput.getShowInAssetPreview());
    }
    return settings;
  }
}
