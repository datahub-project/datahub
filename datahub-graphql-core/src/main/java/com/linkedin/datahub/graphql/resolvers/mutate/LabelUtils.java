package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LabelUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private LabelUtils() { }

  public static final String GLOSSARY_TERM_ASPECT_NAME = "glossaryTerms";
  public static final String SCHEMA_ASPECT_NAME = "editableSchemaMetadata";
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
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(
              targetUrn.toString(), GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms());
      terms.setAuditStamp(getAuditStamp(actor));

      removeTermIfExists(terms, labelUrn);
      persistAspect(targetUrn, terms, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), SCHEMA_ASPECT_NAME, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      removeTermIfExists(editableFieldInfo.getGlossaryTerms(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor, entityService);
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
      persistAspect(targetUrn, tags, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), SCHEMA_ASPECT_NAME, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        editableFieldInfo.setGlobalTags(new GlobalTags());
      }
      removeTagIfExists(editableFieldInfo.getGlobalTags(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor, entityService);
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
      persistAspect(targetUrn, tags, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), SCHEMA_ASPECT_NAME, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        editableFieldInfo.setGlobalTags(new GlobalTags());
      }

      addTagIfNotExists(editableFieldInfo.getGlobalTags(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor, entityService);
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
      persistAspect(targetUrn, terms, actor, entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), SCHEMA_ASPECT_NAME, entityService, new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      editableFieldInfo.getGlossaryTerms().setAuditStamp(getAuditStamp(actor));

      addTermIfNotExistsToEntity(editableFieldInfo.getGlossaryTerms(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor, entityService);
    }
  }

  private static EditableSchemaFieldInfo getFieldInfoFromSchema(
      EditableSchemaMetadata editableSchemaMetadata,
      String fieldPath
  ) {
    if (!editableSchemaMetadata.hasEditableSchemaFieldInfo()) {
      editableSchemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());
    }
    EditableSchemaFieldInfoArray editableSchemaMetadataArray =
        editableSchemaMetadata.getEditableSchemaFieldInfo();
    Optional<EditableSchemaFieldInfo> fieldMetadata = editableSchemaMetadataArray
            .stream()
            .filter(fieldInfo -> fieldInfo.getFieldPath().equals(fieldPath))
            .findFirst();

    if (fieldMetadata.isPresent()) {
      return fieldMetadata.get();
    } else {
      EditableSchemaFieldInfo newFieldInfo = new EditableSchemaFieldInfo();
      newFieldInfo.setFieldPath(fieldPath);
      editableSchemaMetadataArray.add(newFieldInfo);
      return newFieldInfo;
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

    return;
  }

  private static void persistAspect(Urn urn, RecordTemplate aspect, Urn actor, EntityService entityService) {
    Snapshot updatedSnapshot = entityService.buildSnapshot(urn, aspect);
    Entity entityToPersist = new Entity();
    entityToPersist.setValue(updatedSnapshot);
    entityService.ingestEntity(entityToPersist, getAuditStamp(actor));
  }

  private static RecordTemplate getAspectFromEntity(String entityUrn, String aspectName, EntityService entityService, RecordTemplate defaultValue) {
    try {
      RecordTemplate aspect = entityService.getAspect(
          Urn.createFromString(entityUrn),
          aspectName,
          0
      );

      if (aspect == null) {
        return defaultValue;
      }

      return aspect;
    } catch (Exception e) {
      log.error(
          "Error constructing aspect from entity. Entity: {} aspect: {}. Error: {}",
          entityUrn,
          aspectName,
          e.toString()
      );
      e.printStackTrace();
      return null;
    }
  }

  private static AuditStamp getAuditStamp(Urn actor) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actor);
    return auditStamp;
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
        context.getActor(),
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
        context.getActor(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }
}
