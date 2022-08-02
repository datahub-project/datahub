package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


@Slf4j
public class LabelUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private LabelUtils() { }

  public static final String GLOSSARY_TERM_ASPECT_NAME = "glossaryTerms";
  public static final String EDITABLE_SCHEMA_METADATA = "editableSchemaMetadata";
  public static final String TAGS_ASPECT_NAME = "globalTags";

  public static void removeTermFromResource(
      Urn labelUrn,
      Urn resourceUrn,
      String subResource,
      Urn actor,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) MutationUtils.getAspectFromEntity(
              resourceUrn.toString(), GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms());
      terms.setAuditStamp(getAuditStamp(actor));

      removeTermIfExists(terms, labelUrn);
      persistAspect(resourceUrn, GLOSSARY_TERM_ASPECT_NAME, terms, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              resourceUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      removeTermIfExists(editableFieldInfo.getGlossaryTerms(), labelUrn);
      persistAspect(resourceUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
    }
  }

  public static void removeTagsFromResources(
      List<Urn> tags,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildRemoveTagsProposal(tags, resource, actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }

  public static void addTagsToResources(
      List<Urn> tagUrns,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildAddTagsProposal(tagUrns, resource, actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }

  public static void addTermsToResource(
      List<Urn> labelUrns,
      Urn resourceUrn,
      String subResource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(resourceUrn.toString(), GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms());
      terms.setAuditStamp(getAuditStamp(actor));

      if (!terms.hasTerms()) {
        terms.setTerms(new GlossaryTermAssociationArray());
      }

      addTermsIfNotExistsToEntity(terms, labelUrns);
      persistAspect(resourceUrn, GLOSSARY_TERM_ASPECT_NAME, terms, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              resourceUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      editableFieldInfo.getGlossaryTerms().setAuditStamp(getAuditStamp(actor));

      addTermsIfNotExistsToEntity(editableFieldInfo.getGlossaryTerms(), labelUrns);
      persistAspect(resourceUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
    }
  }

  private static void addTermsIfNotExistsToEntity(GlossaryTerms terms, List<Urn> termUrns)
      throws URISyntaxException {
    if (!terms.hasTerms()) {
      terms.setTerms(new GlossaryTermAssociationArray());
    }

    GlossaryTermAssociationArray termArray = terms.getTerms();

    List<Urn> termsToAdd = new ArrayList<>();
    for (Urn termUrn : termUrns) {
      if (termArray.stream().anyMatch(association -> association.getUrn().equals(termUrn))) {
        continue;
      }
      termsToAdd.add(termUrn);
    }

    // Check for no terms to add
    if (termsToAdd.size() == 0) {
      return;
    }

    for (Urn termUrn : termsToAdd) {
      GlossaryTermAssociation newAssociation = new GlossaryTermAssociation();
      newAssociation.setUrn(GlossaryTermUrn.createFromUrn(termUrn));
      termArray.add(newAssociation);
    }
  }

  private static TagAssociationArray removeTagsIfExists(GlobalTags tags, List<Urn> tagUrns) {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    TagAssociationArray tagAssociationArray = tags.getTags();
    for (Urn tagUrn : tagUrns) {
      tagAssociationArray.removeIf(association -> association.getTag().equals(tagUrn));
    }
    return tagAssociationArray;
  }

  private static GlossaryTermAssociationArray removeTermIfExists(GlossaryTerms terms, Urn termUrn) {
    if (!terms.hasTerms()) {
      terms.setTerms(new GlossaryTermAssociationArray());
    }

    GlossaryTermAssociationArray termArray = terms.getTerms();

    termArray.removeIf(association -> association.getUrn().equals(termUrn));
    return termArray;
  }

  public static boolean isAuthorizedToUpdateTags(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.EDIT_DATASET_COL_TAGS_PRIVILEGE.getType()
            : PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToUpdateTerms(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;

    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
                ? PoliciesConfig.EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
                : PoliciesConfig.EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()
            ))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static void validateResourceAndLabel(
      List<Urn> labelUrns,
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      String labelEntityType,
      EntityService entityService,
      Boolean isRemoving
  ) {
    for (Urn urn : labelUrns) {
      validateResourceAndLabel(urn, resourceUrn, subResource, subResourceType, labelEntityType, entityService, isRemoving);
    }
  }

  public static void validateLabel(Urn labelUrn, String labelEntityType, EntityService entityService) {
    if (!labelUrn.getEntityType().equals(labelEntityType)) {
      throw new IllegalArgumentException(String.format("Failed to validate label with urn %s. Urn type does not match entity type %s..",
          labelUrn,
          labelEntityType));
    }
    if (!entityService.exists(labelUrn)) {
      throw new IllegalArgumentException(String.format("Failed to validate label with urn %s. Urn does not exist.", labelUrn));
    }
  }

  public static void validateResource(Urn resourceUrn, String subResource, SubResourceType subResourceType, EntityService entityService) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update resource with urn %s. Entity does not exist.", resourceUrn));
    }
    if ((subResource != null && subResource.length() > 0) || subResourceType != null) {
      if (subResource == null || subResource.length() == 0) {
        throw new IllegalArgumentException(String.format(
            "Failed to update resource with urn %s. SubResourceType (%s) provided without a subResource.", resourceUrn, subResourceType));
      }
      if (subResourceType == null) {
        throw new IllegalArgumentException(String.format(
            "Failed to updates resource with urn %s. SubResource (%s) provided without a subResourceType.",  resourceUrn, subResource));
      }
      validateSubresourceExists(resourceUrn, subResource, subResourceType, entityService);
    }
  }

  public static void validateResourceAndLabel(
      Urn labelUrn,
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      String labelEntityType,
      EntityService entityService,
      Boolean isRemoving
  ) {
    if (!isRemoving) {
      validateLabel(labelUrn, labelEntityType, entityService);
    }
    validateResource(resourceUrn, subResource, subResourceType, entityService);
  }

  private static MetadataChangeProposal buildAddTagsProposal(
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      return buildAddTagsToEntityProposal(tagUrns, resource, actor, entityService);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildAddTagsToSubResourceProposal(tagUrns, resource, actor, entityService);
    }
  }

  private static MetadataChangeProposal buildRemoveTagsProposal(
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      return buildRemoveTagsToEntityProposal(tagUrns, resource, actor, entityService);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildRemoveTagsToSubResourceProposal(tagUrns, resource, actor, entityService);
    }
  }

  private static MetadataChangeProposal buildRemoveTagsToEntityProposal(
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) {
    com.linkedin.common.GlobalTags tags =
        (com.linkedin.common.GlobalTags) getAspectFromEntity(resource.getResourceUrn(), TAGS_ASPECT_NAME, entityService, new GlobalTags());

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    removeTagsIfExists(tags, tagUrns);
    return buildMetadataChangeProposal(UrnUtils.getUrn(resource.getResourceUrn()), TAGS_ASPECT_NAME, tags, actor, entityService);
  }

  private static MetadataChangeProposal buildRemoveTagsToSubResourceProposal(
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
            resource.getResourceUrn(),
            EDITABLE_SCHEMA_METADATA,
            entityService,
            new EditableSchemaMetadata());
    EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (!editableFieldInfo.hasGlobalTags()) {
      editableFieldInfo.setGlobalTags(new GlobalTags());
    }
    removeTagsIfExists(editableFieldInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposal(UrnUtils.getUrn(resource.getResourceUrn()), EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
  }

  private static MetadataChangeProposal buildAddTagsToEntityProposal(
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    com.linkedin.common.GlobalTags tags =
        (com.linkedin.common.GlobalTags) getAspectFromEntity(resource.getResourceUrn(), TAGS_ASPECT_NAME, entityService, new GlobalTags());

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    addTagsIfNotExists(tags, tagUrns);
    return buildMetadataChangeProposal(UrnUtils.getUrn(resource.getResourceUrn()), TAGS_ASPECT_NAME, tags, actor, entityService);
  }

  private static MetadataChangeProposal buildAddTagsToSubResourceProposal(
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
            resource.getResourceUrn(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
    EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (!editableFieldInfo.hasGlobalTags()) {
      editableFieldInfo.setGlobalTags(new GlobalTags());
    }

    addTagsIfNotExists(editableFieldInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposal(UrnUtils.getUrn(resource.getResourceUrn()), EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
  }

  private static void addTagsIfNotExists(GlobalTags tags, List<Urn> tagUrns) throws URISyntaxException {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }

    TagAssociationArray tagAssociationArray = tags.getTags();

    List<Urn> tagsToAdd = new ArrayList<>();
    for (Urn tagUrn : tagUrns) {
      if (tagAssociationArray.stream().anyMatch(association -> association.getTag().equals(tagUrn))) {
        continue;
      }
      tagsToAdd.add(tagUrn);
    }

    // Check for no tags to add
    if (tagsToAdd.size() == 0) {
      return;
    }

    for (Urn tagUrn : tagsToAdd) {
      TagAssociation newAssociation = new TagAssociation();
      newAssociation.setTag(TagUrn.createFromUrn(tagUrn));
      tagAssociationArray.add(newAssociation);
    }
  }

  private static void ingestChangeProposals(List<MetadataChangeProposal> changes, EntityService entityService, Urn actor) {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      entityService.ingestProposal(change, getAuditStamp(actor));
    }
  }
}
