package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LabelUtils {
  private LabelUtils() { }

  @Inject
  @Named("entityService")
  private static EntityService _entityService;

  public static final String GLOSSARY_TERM_ASPECT_NAME = "GlossaryTerms";
  public static final String SCHEMA_ASPECT_NAME = "EditableSchemaMetadata";
  public static final String TAGS_ASPECT_NAME = "GlobalTags";

  private static final Logger _logger = LoggerFactory.getLogger(LabelUtils.class.getName());

  public static Boolean addLabelToEntity(
      String rawLabelUrn,
      String rawTargetUrn,
      String subResource,
      QueryContext context
  ) throws URISyntaxException {

    Urn labelUrn = Urn.createFromString(rawLabelUrn);
    Urn targetUrn = Urn.createFromString(rawTargetUrn);
    Urn actor = CorpuserUrn.createFromString(context.getActor());

    if (labelUrn.getEntityType() != "tag" && labelUrn.getEntityType() != "glossaryTerm") {
      addTagToTarget(labelUrn, targetUrn, subResource, actor);
    } else if (labelUrn.getEntityType() == "glossaryTerm") {
      addTermToTarget(labelUrn, targetUrn, subResource, actor);
    } else {
      throw new RuntimeException(
          "Exception: trying to add type "
              + labelUrn.getEntityType()
              + " via add label endpoint. Types supported: tag, glossaryTerm"
      );
    }
    return true;
  }

  public static Boolean removeLabelFromEntity(
      String rawLabelUrn,
      String rawTargetUrn,
      String subResource,
      QueryContext context
  ) throws URISyntaxException {

    Urn labelUrn = Urn.createFromString(rawLabelUrn);
    Urn targetUrn = Urn.createFromString(rawTargetUrn);
    Urn actor = CorpuserUrn.createFromString(context.getActor());

    if (labelUrn.getEntityType() != "tag" && labelUrn.getEntityType() != "glossaryTerm") {
      removeTagFromTarget(labelUrn, targetUrn, subResource, actor);
    } else if (labelUrn.getEntityType() == "glossaryTerm") {
      removeTermFromTarget(labelUrn, targetUrn, subResource, actor);
    } else {
      throw new RuntimeException(
          "Exception: trying to remove type "
              + labelUrn.getEntityType()
              + " via remove label endpoint. Types supported: tag, glossaryTerm"
      );
    }
    return true;
  }

  private static void removeTermFromTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(targetUrn.toString(), GLOSSARY_TERM_ASPECT_NAME);
      removeTermIfExists(terms.getTerms(), labelUrn);
      persistAspect(targetUrn, terms, actor);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(targetUrn.toString(), SCHEMA_ASPECT_NAME);
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      removeTagIfExists(editableFieldInfo.getGlobalTags().getTags(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor);
    }
  }

  private static void removeTagFromTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags) getAspectFromEntity(targetUrn.toString(), TAGS_ASPECT_NAME);
      removeTagIfExists(tags.getTags(), labelUrn);
      persistAspect(targetUrn, tags, actor);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(targetUrn.toString(), SCHEMA_ASPECT_NAME);
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      removeTagIfExists(editableFieldInfo.getGlobalTags().getTags(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor);
    }
  }

  private static void addTagToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags) getAspectFromEntity(targetUrn.toString(), TAGS_ASPECT_NAME);
      addTagIfNotExists(tags.getTags(), labelUrn);
      persistAspect(targetUrn, tags, actor);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(targetUrn.toString(), SCHEMA_ASPECT_NAME);
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      addTagIfNotExists(editableFieldInfo.getGlobalTags().getTags(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor);
    }
  }

  private static void addTermToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      Urn actor
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(targetUrn.toString(), GLOSSARY_TERM_ASPECT_NAME);
      addTermIfNotExistsToEntity(terms.getTerms(), labelUrn);
      persistAspect(targetUrn, terms, actor);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(targetUrn.toString(), SCHEMA_ASPECT_NAME);
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      addTermIfNotExistsToEntity(editableFieldInfo.getGlossaryTerms().getTerms(), labelUrn);
      persistAspect(targetUrn, editableSchemaMetadata, actor);
    }
  }

  private static EditableSchemaFieldInfo getFieldInfoFromSchema(
      EditableSchemaMetadata editableSchemaMetadata,
      String fieldPath
  ) {
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

  private static GlossaryTermAssociationArray addTermIfNotExistsToEntity(GlossaryTermAssociationArray terms, Urn termUrn) {
    // if term exists, do not add it again
    if (terms.stream().anyMatch(association -> association.getUrn().equals(termUrn))) {
      return terms;
    }

    GlossaryTermAssociation newAssociation = new GlossaryTermAssociation();
    newAssociation.setUrn((GlossaryTermUrn) termUrn);
    terms.add(newAssociation);

    return terms;
  }

  private static TagAssociationArray removeTagIfExists(TagAssociationArray tags, Urn tagUrn) {
    tags.removeIf(association -> association.getTag().equals(tagUrn));
    return tags;
  }

  private static GlossaryTermAssociationArray removeTermIfExists(GlossaryTermAssociationArray terms, Urn termUrn) {
    terms.removeIf(association -> association.getUrn().equals(termUrn));
    return terms;
  }

  private static TagAssociationArray addTagIfNotExists(TagAssociationArray tags, Urn tagUrn) {
    // if tag exists, do not add it again
    if (tags.stream().anyMatch(association -> association.getTag().equals(tagUrn))) {
      return tags;
    }

    TagAssociation newAssociation = new TagAssociation();
    newAssociation.setTag((TagUrn) tagUrn);
    tags.add(newAssociation);

    return tags;
  }

  private static void persistAspect(Urn urn, RecordTemplate aspect, Urn actor) {
    Snapshot updatedSnapshot = _entityService.buildSnapshot(urn, aspect);
    Entity entityToPersist = new Entity();
    entityToPersist.setValue(updatedSnapshot);
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actor);
    _entityService.ingestEntity(entityToPersist, auditStamp);
  }

  private static Object getAspectFromEntity(String entityUrn, String aspectName) {
    try {
      Entity beforeEntity = _entityService.getEntity(
          Urn.createFromString(entityUrn),
          ImmutableSet.of(aspectName)
      );

      return ResolverUtils.constructAspectFromDataElement(
          AspectExtractor.extractAspects(beforeEntity).get(aspectName)
      );
    } catch (Exception e) {
      _logger.error(
          "Error constructing aspect from entity. Entity: {} aspect: {}. Error: {}",
          entityUrn,
          aspectName,
          e.toString()
      );
      e.printStackTrace();
      return null;
    }
  }
}
