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
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
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

  public static void removeTermFromTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) MutationUtils.getAspectFromEntity(
              targetUrn.toString(), GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms());
      terms.setAuditStamp(getAuditStamp(actor));

      removeTermIfExists(terms, labelUrn);
      persistAspect(targetUrn, GLOSSARY_TERM_ASPECT_NAME, terms, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      removeTermIfExists(editableFieldInfo.getGlossaryTerms(), labelUrn);
      persistAspect(targetUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
    }
  }

  public static void removeTagFromTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags) getAspectFromEntity(targetUrn.toString(), TAGS_ASPECT_NAME, entityService, new GlobalTags());

      if (!tags.hasTags()) {
        tags.setTags(new TagAssociationArray());
      }
      removeTagIfExists(tags, labelUrn);
      persistAspect(targetUrn, TAGS_ASPECT_NAME, tags, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        editableFieldInfo.setGlobalTags(new GlobalTags());
      }
      removeTagIfExists(editableFieldInfo.getGlobalTags(), labelUrn);
      persistAspect(targetUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
    }
  }

  public static void addTagToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags) getAspectFromEntity(targetUrn.toString(), TAGS_ASPECT_NAME, entityService, new GlobalTags());

      if (!tags.hasTags()) {
        tags.setTags(new TagAssociationArray());
      }
      addTagIfNotExists(tags, labelUrn);
      persistAspect(targetUrn, TAGS_ASPECT_NAME, tags, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        editableFieldInfo.setGlobalTags(new GlobalTags());
      }

      addTagIfNotExists(editableFieldInfo.getGlobalTags(), labelUrn);
      persistAspect(targetUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
    }
  }

  public static void addTermToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor,
      EntityService entityService
  ) throws URISyntaxException {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(targetUrn.toString(), GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms());
      terms.setAuditStamp(getAuditStamp(actor));

      if (!terms.hasTerms()) {
        terms.setTerms(new GlossaryTermAssociationArray());
      }

      addTermIfNotExistsToEntity(terms, labelUrn);
      persistAspect(targetUrn, GLOSSARY_TERM_ASPECT_NAME, terms, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      editableFieldInfo.getGlossaryTerms().setAuditStamp(getAuditStamp(actor));

      addTermIfNotExistsToEntity(editableFieldInfo.getGlossaryTerms(), labelUrn);
      persistAspect(targetUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
    }
  }

  private static void addTermIfNotExistsToEntity(GlossaryTerms terms, Urn termUrn)
      throws URISyntaxException {
    if (!terms.hasTerms()) {
      terms.setTerms(new GlossaryTermAssociationArray());
    }

    GlossaryTermAssociationArray termArray = terms.getTerms();

    // if term exists, do not add it again
    if (termArray.stream().anyMatch(association -> association.getUrn().equals(termUrn))) {
      return;
    }

    GlossaryTermAssociation newAssociation = new GlossaryTermAssociation();
    newAssociation.setUrn(GlossaryTermUrn.createFromUrn(termUrn));
    termArray.add(newAssociation);
  }

  private static TagAssociationArray removeTagIfExists(GlobalTags tags, Urn tagUrn) {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }

    TagAssociationArray tagAssociationArray = tags.getTags();

    tagAssociationArray.removeIf(association -> association.getTag().equals(tagUrn));
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

  private static void addTagIfNotExists(GlobalTags tags, Urn tagUrn) throws URISyntaxException {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }

    TagAssociationArray tagAssociationArray = tags.getTags();

    // if tag exists, do not add it again
    if (tagAssociationArray.stream().anyMatch(association -> association.getTag().equals(tagUrn))) {
      return;
    }

    TagAssociation newAssociation = new TagAssociation();
    newAssociation.setTag(TagUrn.createFromUrn(tagUrn));
    tagAssociationArray.add(newAssociation);
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

  public static Boolean validateInput(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      String labelEntityType,
      EntityService entityService,
      Boolean isRemoving
  ) {
    if (!labelUrn.getEntityType().equals(labelEntityType)) {
      throw new IllegalArgumentException(String.format("Failed to update %s on %s. Was expecting a %s.", labelUrn, targetUrn, labelEntityType));
    }

    if (!entityService.exists(targetUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s on %s. %s does not exist.", labelUrn, targetUrn, targetUrn));
    }

    // Datahub allows removing tags & terms it is not familiar with- however these terms must exist to be added back
    if (!entityService.exists(labelUrn) && !isRemoving) {
      throw new IllegalArgumentException(String.format("Failed to update %s on %s. %s does not exist.", labelUrn, targetUrn, labelUrn));
    }

    if ((subResource != null && subResource.length() > 0) || subResourceType != null) {
      if (subResource == null || subResource.length() == 0) {
        throw new IllegalArgumentException(String.format(
            "Failed to update %s on %s. SubResourceType (%s) provided without a subResource.", labelUrn, targetUrn, subResourceType));
      }
      if (subResourceType == null) {
        throw new IllegalArgumentException(String.format(
            "Failed to update %s on %s. SubResource (%s) provided without a subResourceType.", labelUrn, targetUrn, subResource));
      }
      validateSubresourceExists(targetUrn, subResource, subResourceType, entityService);
    }

    return true;
  }
}
