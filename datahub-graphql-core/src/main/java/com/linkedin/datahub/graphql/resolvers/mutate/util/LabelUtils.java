package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.businessattribute.BusinessAttributeInfo;
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
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

// TODO: Move to consuming GlossaryTermService, TagService.
@Slf4j
public class LabelUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private LabelUtils() {}

  public static void removeTermFromResource(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      Urn resourceUrn,
      String subResource,
      Urn actor,
      EntityService<?> entityService) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  resourceUrn.toString(),
                  Constants.GLOSSARY_TERMS_ASPECT_NAME,
                  entityService,
                  new GlossaryTerms());
      terms.setAuditStamp(EntityUtils.getAuditStamp(actor));

      removeTermIfExists(terms, labelUrn);
      persistAspect(
          opContext,
          resourceUrn,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          terms,
          actor,
          entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  resourceUrn.toString(),
                  Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                  entityService,
                  new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo =
          getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      removeTermIfExists(editableFieldInfo.getGlossaryTerms(), labelUrn);
      persistAspect(
          opContext,
          resourceUrn,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          editableSchemaMetadata,
          actor,
          entityService);
    }
  }

  public static void removeTagsFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> tags,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService<?> entityService)
      throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildRemoveTagsProposal(opContext, tags, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  /**
   * Optimized batch version of addTagsToResources that reduces N+1 queries. Batch-reads all aspects
   * instead of reading per-resource.
   */
  public static void addTagsToResources(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService<?> entityService)
      throws Exception {
    Map<Boolean, List<ResourceRefInput>> partitioned =
        resources.stream()
            .collect(
                Collectors.partitioningBy(
                    r -> r.getSubResource() == null || r.getSubResource().isEmpty()));

    List<ResourceRefInput> entityResources = partitioned.get(true);
    List<ResourceRefInput> subResourcesList = partitioned.get(false);

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    Map<Urn, GlobalTags> globalTagsByUrn =
        batchReadAspect(
            opContext,
            entityResources,
            Constants.GLOBAL_TAGS_ASPECT_NAME,
            GlobalTags.class,
            GlobalTags::new,
            entityService);

    List<ResourceRefInput> businessEntityResources =
        entityResources.stream()
            .filter(
                r -> {
                  Urn urn = UrnUtils.getUrn(r.getResourceUrn());
                  return urn.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME);
                })
            .toList();

    Map<Urn, BusinessAttributeInfo> businessAttributeTagsByUrn =
        batchReadAspect(
            opContext,
            businessEntityResources,
            Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
            BusinessAttributeInfo.class,
            BusinessAttributeInfo::new,
            entityService);

    for (ResourceRefInput resource : entityResources) {
      Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
      if (resourceUrn.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
        BusinessAttributeInfo businessAttributeInfo =
            businessAttributeTagsByUrn.getOrDefault(resourceUrn, new BusinessAttributeInfo());
        changes.add(buildMCPForBusinessProposal(businessAttributeInfo, tagUrns, resource));
      } else {
        GlobalTags tags = globalTagsByUrn.getOrDefault(resourceUrn, new GlobalTags());
        changes.add(buildMcp(tags, tagUrns, resource));
      }
    }

    Map<Urn, EditableSchemaMetadata> schemaByUrn =
        batchReadAspect(
            opContext,
            subResourcesList,
            Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            EditableSchemaMetadata.class,
            EditableSchemaMetadata::new,
            entityService);

    Map<Urn, List<ResourceRefInput>> subResourcesByDataset =
        subResourcesList.stream()
            .collect(
                Collectors.groupingBy(
                    r -> UrnUtils.getUrn(r.getResourceUrn()), Collectors.toList()));

    for (Map.Entry<Urn, List<ResourceRefInput>> entry : subResourcesByDataset.entrySet()) {
      Urn datasetUrn = entry.getKey();
      List<ResourceRefInput> fieldsInDataset = entry.getValue();
      EditableSchemaMetadata schema =
          schemaByUrn.getOrDefault(datasetUrn, new EditableSchemaMetadata());

      for (ResourceRefInput resource : fieldsInDataset) {
        EditableSchemaFieldInfo fieldInfo =
            getFieldInfoFromSchema(schema, resource.getSubResource());
        if (fieldInfo.getGlobalTags() == null) {
          fieldInfo.setGlobalTags(new GlobalTags());
        }
        addTagsIfNotExists(fieldInfo.getGlobalTags(), tagUrns);
      }

      changes.add(
          buildMetadataChangeProposalWithUrn(
              datasetUrn, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME, schema));
    }

    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  /**
   * Generic batch-reader for aspects across multiple entities.
   *
   * <p>Error Handling: If batch read fails, gracefully falls back to returning default values for
   * all resources (prevents cascading failures).
   *
   * @param <T> the aspect type (must extend RecordTemplate)
   * @param opContext the operation context
   * @param resources the list of resources to read aspects for
   * @param aspectName the name of the aspect to read (e.g., Constants.GLOBAL_TAGS_ASPECT_NAME)
   * @param aspectClass the class of the aspect for type checking
   * @param defaultSupplier supplier that creates default instances when aspect is missing
   * @param entityService the entity service
   * @return map of URN to aspect (default values if read fails)
   */
  private static <T extends RecordTemplate> Map<Urn, T> batchReadAspect(
      @Nonnull OperationContext opContext,
      List<ResourceRefInput> resources,
      String aspectName,
      Class<T> aspectClass,
      Supplier<T> defaultSupplier,
      EntityService<?> entityService) {
    Map<Urn, T> result = new HashMap<>();
    if (resources.isEmpty()) {
      return result;
    }

    try {
      Set<Urn> resourceUrns =
          resources.stream()
              .map(r -> UrnUtils.getUrn(r.getResourceUrn()))
              .collect(Collectors.toSet());

      Map<Urn, List<RecordTemplate>> allAspects =
          entityService.getLatestAspects(
              opContext, resourceUrns, new HashSet<>(List.of(aspectName)), false);

      for (Map.Entry<Urn, List<RecordTemplate>> entry : allAspects.entrySet()) {
        if (entry.getValue() != null && !entry.getValue().isEmpty()) {
          RecordTemplate aspectRecord = entry.getValue().get(0);
          if (aspectClass.isInstance(aspectRecord)) {
            result.put(entry.getKey(), aspectClass.cast(aspectRecord));
          } else {
            result.put(entry.getKey(), defaultSupplier.get());
          }
        } else {
          result.put(entry.getKey(), defaultSupplier.get());
        }
      }
    } catch (Exception e) {
      log.error("Failed to batch-read {}, throwing error", aspectName, e);
      throw new RuntimeException(e);
    }
    return result;
  }

  public static void removeTermsFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService<?> entityService)
      throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildRemoveTermsProposal(opContext, termUrns, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  public static void addTermsToResources(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService<?> entityService)
      throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildAddTermsProposal(opContext, termUrns, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  public static void addTermsToResource(
      @Nonnull OperationContext opContext,
      List<Urn> labelUrns,
      Urn resourceUrn,
      String subResource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  resourceUrn.toString(),
                  Constants.GLOSSARY_TERMS_ASPECT_NAME,
                  entityService,
                  new GlossaryTerms());
      terms.setAuditStamp(EntityUtils.getAuditStamp(actor));

      if (!terms.hasTerms()) {
        terms.setTerms(new GlossaryTermAssociationArray());
      }

      addTermsIfNotExists(terms, labelUrns);
      persistAspect(
          opContext,
          resourceUrn,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          terms,
          actor,
          entityService);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  resourceUrn.toString(),
                  Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                  entityService,
                  new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo =
          getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      editableFieldInfo.getGlossaryTerms().setAuditStamp(EntityUtils.getAuditStamp(actor));

      addTermsIfNotExists(editableFieldInfo.getGlossaryTerms(), labelUrns);
      persistAspect(
          opContext,
          resourceUrn,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          editableSchemaMetadata,
          actor,
          entityService);
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

  public static boolean isAuthorizedToUpdateTags(
      @Nonnull QueryContext context, Urn targetUrn, String subResource, Collection<Urn> tagUrns) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.EDIT_DATASET_COL_TAGS_PRIVILEGE.getType()
                            : PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorizedForTags(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups, tagUrns);
  }

  public static boolean isAuthorizedToUpdateTerms(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;

    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
                            : PoliciesConfig.EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static void validateResourceAndLabel(
      @Nonnull OperationContext opContext,
      List<Urn> labelUrns,
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      String labelEntityType,
      EntityService<?> entityService,
      Boolean isRemoving) {
    for (Urn urn : labelUrns) {
      validateResourceAndLabel(
          opContext,
          urn,
          resourceUrn,
          subResource,
          subResourceType,
          labelEntityType,
          entityService,
          isRemoving);
    }
  }

  public static void validateLabel(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      String labelEntityType,
      EntityService<?> entityService) {
    if (!labelUrn.getEntityType().equals(labelEntityType)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to validate label with urn %s. Urn type does not match entity type %s..",
              labelUrn, labelEntityType));
    }
    if (!entityService.exists(opContext, labelUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to validate label with urn %s. Urn does not exist.", labelUrn));
    }
  }

  // TODO: Move this out into a separate utilities class.
  public static void validateResource(
      @Nonnull OperationContext opContext,
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update resource with urn %s. Entity does not exist.", resourceUrn));
    }
    if ((subResource != null && subResource.length() > 0) || subResourceType != null) {
      if (subResource == null || subResource.length() == 0) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to update resource with urn %s. SubResourceType (%s) provided without a subResource.",
                resourceUrn, subResourceType));
      }
      if (subResourceType == null) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to updates resource with urn %s. SubResource (%s) provided without a subResourceType.",
                resourceUrn, subResource));
      }
      validateSubresourceExists(
          opContext, resourceUrn, subResource, subResourceType, entityService);
    }
  }

  public static void validateResourceAndLabel(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      String labelEntityType,
      EntityService<?> entityService,
      Boolean isRemoving) {
    if (!isRemoving) {
      validateLabel(opContext, labelUrn, labelEntityType, entityService);
    }
    validateResource(opContext, resourceUrn, subResource, subResourceType, entityService);
  }

  private static MetadataChangeProposal buildAddTagsProposal(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      Urn targetUrn = Urn.createFromString(resource.getResourceUrn());
      if (targetUrn.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
        return buildAddTagsToBusinessAttributeProposal(
            opContext, tagUrns, resource, actor, entityService);
      }
      return buildAddTagsToEntityProposal(opContext, tagUrns, resource, actor, entityService);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildAddTagsToSubResourceProposal(opContext, tagUrns, resource, actor, entityService);
    }
  }

  private static MetadataChangeProposal buildRemoveTagsProposal(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      Urn targetUrn = Urn.createFromString(resource.getResourceUrn());
      if (targetUrn.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
        return buildRemoveTagsToBusinessAttributeProposal(
            opContext, tagUrns, resource, actor, entityService);
      }
      return buildRemoveTagsToEntityProposal(opContext, tagUrns, resource, actor, entityService);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildRemoveTagsToSubResourceProposal(
          opContext, tagUrns, resource, actor, entityService);
    }
  }

  private static MetadataChangeProposal buildRemoveTagsToEntityProposal(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    com.linkedin.common.GlobalTags tags =
        (com.linkedin.common.GlobalTags)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.GLOBAL_TAGS_ASPECT_NAME,
                entityService,
                new GlobalTags());

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    removeTagsIfExists(tags, tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), Constants.GLOBAL_TAGS_ASPECT_NAME, tags);
  }

  private static MetadataChangeProposal buildRemoveTagsToSubResourceProposal(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                entityService,
                new EditableSchemaMetadata());
    EditableSchemaFieldInfo editableFieldInfo =
        getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (!editableFieldInfo.hasGlobalTags()) {
      editableFieldInfo.setGlobalTags(new GlobalTags());
    }
    removeTagsIfExists(editableFieldInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }

  private static MetadataChangeProposal buildAddTagsToEntityProposal(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    com.linkedin.common.GlobalTags tags =
        (com.linkedin.common.GlobalTags)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.GLOBAL_TAGS_ASPECT_NAME,
                entityService,
                new GlobalTags());

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    addTagsIfNotExists(tags, tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), Constants.GLOBAL_TAGS_ASPECT_NAME, tags);
  }

  private static MetadataChangeProposal buildAddTagsToSubResourceProposal(
      @Nonnull OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                entityService,
                new EditableSchemaMetadata());
    EditableSchemaFieldInfo editableFieldInfo =
        getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (!editableFieldInfo.hasGlobalTags()) {
      editableFieldInfo.setGlobalTags(new GlobalTags());
    }

    addTagsIfNotExists(editableFieldInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }

  private static void addTagsIfNotExists(GlobalTags tags, List<Urn> tagUrns)
      throws URISyntaxException {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }

    TagAssociationArray tagAssociationArray = tags.getTags();

    List<Urn> tagsToAdd = new ArrayList<>();
    for (Urn tagUrn : tagUrns) {
      if (tagAssociationArray.stream()
          .anyMatch(association -> association.getTag().equals(tagUrn))) {
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

  private static MetadataChangeProposal buildAddTermsProposal(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding terms to a top-level entity
      Urn targetUrn = Urn.createFromString(resource.getResourceUrn());
      if (targetUrn.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
        return buildAddTermsToBusinessAttributeProposal(
            opContext, termUrns, resource, actor, entityService);
      }
      return buildAddTermsToEntityProposal(opContext, termUrns, resource, actor, entityService);
    } else {
      // Case 2: Adding terms to subresource (e.g. schema fields)
      return buildAddTermsToSubResourceProposal(
          opContext, termUrns, resource, actor, entityService);
    }
  }

  private static MetadataChangeProposal buildRemoveTermsProposal(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Removing terms from a top-level entity
      Urn targetUrn = Urn.createFromString(resource.getResourceUrn());
      if (targetUrn.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
        return buildRemoveTermsToBusinessAttributeProposal(
            opContext, termUrns, resource, actor, entityService);
      }
      return buildRemoveTermsToEntityProposal(opContext, termUrns, resource, actor, entityService);
    } else {
      // Case 2: Removing terms from subresource (e.g. schema fields)
      return buildRemoveTermsToSubResourceProposal(
          opContext, termUrns, resource, actor, entityService);
    }
  }

  private static MetadataChangeProposal buildAddTermsToEntityProposal(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    com.linkedin.common.GlossaryTerms terms =
        (com.linkedin.common.GlossaryTerms)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.GLOSSARY_TERMS_ASPECT_NAME,
                entityService,
                new GlossaryTerms());
    terms.setAuditStamp(EntityUtils.getAuditStamp(actor));

    if (!terms.hasTerms()) {
      terms.setTerms(new GlossaryTermAssociationArray());
    }

    addTermsIfNotExists(terms, termUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), Constants.GLOSSARY_TERMS_ASPECT_NAME, terms);
  }

  private static MetadataChangeProposal buildAddTermsToSubResourceProposal(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                entityService,
                new EditableSchemaMetadata());

    EditableSchemaFieldInfo editableFieldInfo =
        getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());
    if (!editableFieldInfo.hasGlossaryTerms()) {
      editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
    }

    editableFieldInfo.getGlossaryTerms().setAuditStamp(EntityUtils.getAuditStamp(actor));

    addTermsIfNotExists(editableFieldInfo.getGlossaryTerms(), termUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }

  private static MetadataChangeProposal buildRemoveTermsToEntityProposal(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    com.linkedin.common.GlossaryTerms terms =
        (com.linkedin.common.GlossaryTerms)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.GLOSSARY_TERMS_ASPECT_NAME,
                entityService,
                new GlossaryTerms());
    terms.setAuditStamp(EntityUtils.getAuditStamp(actor));

    removeTermsIfExists(terms, termUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), Constants.GLOSSARY_TERMS_ASPECT_NAME, terms);
  }

  private static MetadataChangeProposal buildRemoveTermsToSubResourceProposal(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        (com.linkedin.schema.EditableSchemaMetadata)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                entityService,
                new EditableSchemaMetadata());
    EditableSchemaFieldInfo editableFieldInfo =
        getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());
    if (!editableFieldInfo.hasGlossaryTerms()) {
      editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
    }

    removeTermsIfExists(editableFieldInfo.getGlossaryTerms(), termUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }

  private static void addTermsIfNotExists(GlossaryTerms terms, List<Urn> termUrns)
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

  private static GlossaryTermAssociationArray removeTermsIfExists(
      GlossaryTerms terms, List<Urn> termUrns) {
    if (!terms.hasTerms()) {
      terms.setTerms(new GlossaryTermAssociationArray());
    }
    GlossaryTermAssociationArray termAssociationArray = terms.getTerms();
    for (Urn termUrn : termUrns) {
      termAssociationArray.removeIf(association -> association.getUrn().equals(termUrn));
    }
    return termAssociationArray;
  }

  private static MetadataChangeProposal buildAddTagsToBusinessAttributeProposal(
      OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    BusinessAttributeInfo businessAttributeInfo =
        (BusinessAttributeInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                entityService,
                new GlobalTags());

    if (!businessAttributeInfo.hasGlobalTags()) {
      businessAttributeInfo.setGlobalTags(new GlobalTags());
    }
    addTagsIfNotExists(businessAttributeInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        businessAttributeInfo);
  }

  private static MetadataChangeProposal buildAddTermsToBusinessAttributeProposal(
      OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService)
      throws URISyntaxException {
    BusinessAttributeInfo businessAttributeInfo =
        (BusinessAttributeInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                entityService,
                new GlossaryTerms());
    if (!businessAttributeInfo.hasGlossaryTerms()) {
      businessAttributeInfo.setGlossaryTerms(new GlossaryTerms());
    }
    businessAttributeInfo.getGlossaryTerms().setAuditStamp(EntityUtils.getAuditStamp(actor));
    addTermsIfNotExists(businessAttributeInfo.getGlossaryTerms(), termUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        businessAttributeInfo);
  }

  private static MetadataChangeProposal buildRemoveTagsToBusinessAttributeProposal(
      OperationContext opContext,
      List<Urn> tagUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    BusinessAttributeInfo businessAttributeInfo =
        (BusinessAttributeInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                entityService,
                new GlobalTags());

    if (!businessAttributeInfo.hasGlobalTags()) {
      businessAttributeInfo.setGlobalTags(new GlobalTags());
    }
    removeTagsIfExists(businessAttributeInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        businessAttributeInfo);
  }

  private static MetadataChangeProposal buildRemoveTermsToBusinessAttributeProposal(
      OperationContext opContext,
      List<Urn> termUrns,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    BusinessAttributeInfo businessAttributeInfo =
        (BusinessAttributeInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                entityService,
                new GlossaryTerms());
    if (!businessAttributeInfo.hasGlossaryTerms()) {
      businessAttributeInfo.setGlossaryTerms(new GlossaryTerms());
    }
    removeTermsIfExists(businessAttributeInfo.getGlossaryTerms(), termUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        businessAttributeInfo);
  }

  public static void validateLabels(
      @Nonnull OperationContext opContext,
      List<Urn> labelUrns,
      String labelEntityType,
      EntityService<?> entityService) {
    for (Urn labelUrn : labelUrns) {
      if (!labelUrn.getEntityType().equals(labelEntityType)) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to validate label with urn %s. Urn type does not match entity type %s..",
                labelUrn, labelEntityType));
      }
    }
    Map<String, Urn> existingUrns =
        entityService.exists(opContext, labelUrns, true).stream()
            .collect(Collectors.toMap(Urn::toString, Function.identity()));
    for (Urn labelUrn : labelUrns) {
      if (!existingUrns.containsKey(labelUrn.toString())) {
        throw new IllegalArgumentException(
            String.format("Failed to validate label with urn %s. Urn does not exist.", labelUrn));
      }
    }
  }

  private static MetadataChangeProposal buildMcp(
      GlobalTags tags, List<Urn> tagUrns, ResourceRefInput resource) throws URISyntaxException {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    addTagsIfNotExists(tags, tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), Constants.GLOBAL_TAGS_ASPECT_NAME, tags);
  }

  private static MetadataChangeProposal buildMCPForBusinessProposal(
      BusinessAttributeInfo businessAttributeInfo, List<Urn> tagUrns, ResourceRefInput resource)
      throws URISyntaxException {
    if (!businessAttributeInfo.hasGlobalTags()) {
      businessAttributeInfo.setGlobalTags(new GlobalTags());
    }
    addTagsIfNotExists(businessAttributeInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()),
        Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        businessAttributeInfo);
  }

  public record UrnSubResource(Urn urn, SubResourceType subResourceType, String subresource) {}

  public static void validateResources(
      @Nonnull OperationContext opContext,
      List<ResourceRefInput> resources,
      EntityService<?> entityService) {
    if (CollectionUtils.isEmpty(resources)) {
      return;
    }
    List<Urn> urns = resources.stream().map(r -> UrnUtils.getUrn(r.getResourceUrn())).toList();
    Map<String, Urn> existingUrns =
        entityService.exists(opContext, urns, true).stream()
            .collect(Collectors.toMap(Urn::toString, Function.identity()));
    for (Urn urn : urns) {
      if (!existingUrns.containsKey(urn.toString())) {
        throw new IllegalArgumentException(
            String.format("Failed to validate resource with urn %s. Urn does not exist.", urn));
      }
    }

    List<UrnSubResource> subResourcesToValidate = new ArrayList<>();
    for (int i = 0; i < resources.size(); i++) {
      ResourceRefInput resource = resources.get(i);
      Urn resourceUrn = urns.get(i);
      String subResource = resource.getSubResource();
      SubResourceType subResourceType = resource.getSubResourceType();
      if (!StringUtils.isEmpty(subResource) || subResourceType != null) {
        if (StringUtils.isEmpty(subResource)) {
          throw new IllegalArgumentException(
              String.format(
                  "Failed to update resource with urn %s. SubResourceType (%s) provided without a subResource.",
                  resourceUrn, subResourceType));
        }
        if (subResourceType == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Failed to updates resource with urn %s. SubResource (%s) provided without a subResourceType.",
                  resourceUrn, subResource));
        }
        subResourcesToValidate.add(new UrnSubResource(resourceUrn, subResourceType, subResource));
      }
    }
    validateSubresourcesExists(opContext, subResourcesToValidate, entityService);
  }
}
