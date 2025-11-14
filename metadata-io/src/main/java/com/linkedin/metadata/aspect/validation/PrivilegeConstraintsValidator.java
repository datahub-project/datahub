package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.UI_SOURCE;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.util.RecordUtils;
import com.google.common.collect.Streams;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class PrivilegeConstraintsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;
  private boolean domainBasedAuthorizationEnabled = false;

  @Override
  protected Stream<AspectValidationException> validateProposedAspectsWithOriginalBatch(
      @Nonnull Collection<? extends BatchItem> filteredBatch,
      @Nonnull Collection<? extends BatchItem> originalBatch,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    if (session == null) {
      exceptions.addException(
          filteredBatch.stream().findFirst().orElseThrow(IllegalStateException::new),
          "No authentication details found, cannot authorize change.");
      return exceptions.streamAllExceptions();
    }

    // Process only the filtered batch (aspects this validator should handle)
    // Use originalBatch for cross-aspect lookups (e.g., finding domains)
    for (BatchItem item : filteredBatch) {
      if (item.getSystemMetadata() != null
          && item.getSystemMetadata().getProperties() != null
          && UI_SOURCE.equals(item.getSystemMetadata().getProperties().get(APP_SOURCE))) {
        // We skip UI events, these are handled by LabelUtils.
        continue;
      }
      AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
      switch (item.getAspectName()) {
        case GLOBAL_TAGS_ASPECT_NAME:
          validateGlobalTags(
                  session,
                  item,
                  originalBatch, // Use originalBatch for domain lookups
                  aspectRetriever,
                  aspectRetriever.getLatestAspectObject(item.getUrn(), GLOBAL_TAGS_ASPECT_NAME))
              .forEach(exceptions::addException);
          break;
        case SCHEMA_METADATA_ASPECT_NAME:
          validateSchemaMetadata(
                  session,
                  item,
                  originalBatch, // Use originalBatch for domain lookups
                  aspectRetriever,
                  aspectRetriever.getLatestAspectObject(item.getUrn(), SCHEMA_METADATA_ASPECT_NAME))
              .forEach(exceptions::addException);
          break;
        case EDITABLE_SCHEMA_METADATA_ASPECT_NAME:
          validateEditableSchemaMetadata(
                  session,
                  item,
                  originalBatch, // Use originalBatch for domain lookups
                  aspectRetriever,
                  aspectRetriever.getLatestAspectObject(
                      item.getUrn(), EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
              .forEach(exceptions::addException);
          break;
        default:
          log.warn("Triggered PrivilegeConstraints Validator for unsupported aspect, ignoring.");
      }
    }

    return exceptions.streamAllExceptions();
  }

  private List<AspectValidationException> validateGlobalTags(
      AuthorizationSession session,
      BatchItem item,
      Collection<? extends BatchItem> allBatchItems,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentTagsAspect) {
    GlobalTags newTags;
    GlobalTags currentTags =
        currentTagsAspect == null
            ? null
            : RecordUtils.toRecordTemplate(GlobalTags.class, currentTagsAspect.data());
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof ProposedItem) {
      ProposedItem proposedItem = (ProposedItem) item;
      PatchItemImpl patchItem =
          PatchItemImpl.builder()
              .build(
                  proposedItem.getMetadataChangeProposal(),
                  proposedItem.getAuditStamp(),
                  aspectRetriever.getEntityRegistry());
      ;
      newTags = patchItem.applyPatch(currentTags, aspectRetriever).getAspect(GlobalTags.class);
    } else {
      newTags = item.getAspect(GlobalTags.class);
    }
    if (newTags != null) {
      Set<Urn> tagDifference = extractTagDifference(newTags, currentTags);

      // Combine tags + domains (if enabled) as subResources for authorization check
      Set<Urn> subResources = new HashSet<>(tagDifference);

      // Only collect domain information if domain-based authorization is enabled
      if (domainBasedAuthorizationEnabled) {
        Set<Urn> domainUrns =
            getEntityDomainsFromBatchOrDB(item.getUrn(), allBatchItems, aspectRetriever);
        subResources.addAll(domainUrns);
      }

      if (!AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
          session,
          ApiOperation.fromChangeType(item.getChangeType()),
          List.of(item.getUrn()),
          subResources)) {
        return List.of(
            AspectValidationException.forItem(
                item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
      }
    }
    return Collections.emptyList();
  }

  private Set<Urn> extractTagDifference(GlobalTags newTags, @Nullable GlobalTags currentTags) {
    Set<Urn> tagUrns =
        Optional.ofNullable(newTags.getTags(GetMode.NULL))
            .orElse(new TagAssociationArray())
            .stream()
            .map(TagAssociation::getTag)
            .collect(Collectors.toSet());
    Set<Urn> tagDifference;
    if (currentTags != null) {
      Set<Urn> existingTagUrns =
          Optional.ofNullable(currentTags.getTags(GetMode.NULL))
              .orElse(new TagAssociationArray())
              .stream()
              .map(TagAssociation::getTag)
              .collect(Collectors.toSet());
      tagDifference =
          Streams.concat(
                  // Tag is being removed from current state
                  existingTagUrns.stream().filter(urn -> !tagUrns.contains(urn)),
                  // Tag is being added to current state
                  tagUrns.stream().filter(urn -> !existingTagUrns.contains(urn)))
              .collect(Collectors.toSet());
    } else {
      // State does not currently exist, all tags are being added
      tagDifference = tagUrns;
    }
    return tagDifference;
  }

  private List<AspectValidationException> validateSchemaMetadata(
      AuthorizationSession session,
      BatchItem item,
      Collection<? extends BatchItem> allBatchItems,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentSchemaAspect) {
    SchemaMetadata schemaMetadata;
    SchemaMetadata currentSchema =
        currentSchemaAspect == null
            ? null
            : RecordUtils.toRecordTemplate(SchemaMetadata.class, currentSchemaAspect.data());
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof ProposedItem) {
      ProposedItem proposedItem = (ProposedItem) item;
      PatchItemImpl patchItem =
          PatchItemImpl.builder()
              .build(
                  proposedItem.getMetadataChangeProposal(),
                  proposedItem.getAuditStamp(),
                  aspectRetriever.getEntityRegistry());
      schemaMetadata =
          patchItem.applyPatch(currentSchema, aspectRetriever).getAspect(SchemaMetadata.class);
    } else {
      schemaMetadata = item.getAspect(SchemaMetadata.class);
    }
    if (schemaMetadata != null) {
      final Map<String, GlobalTags> existingTagsMap = new HashMap<>();
      if (currentSchema != null) {
        existingTagsMap.putAll(
            currentSchema.getFields().stream()
                .collect(
                    Collectors.toMap(
                        SchemaField::getFieldPath,
                        schemaField ->
                            Optional.ofNullable(schemaField.getGlobalTags())
                                .orElse(new GlobalTags()))));
      }
      Set<Urn> tagDifference =
          schemaMetadata.getFields().stream()
              .map(
                  schemaField ->
                      extractTagDifference(
                          Optional.ofNullable(schemaField.getGlobalTags()).orElse(new GlobalTags()),
                          existingTagsMap.get(schemaField.getFieldPath())))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      // Combine tags + domains (if enabled) as subResources for authorization check
      Set<Urn> subResources = new HashSet<>(tagDifference);

      // Only collect domain information if domain-based authorization is enabled
      if (domainBasedAuthorizationEnabled) {
        Set<Urn> domainUrns =
            getEntityDomainsFromBatchOrDB(item.getUrn(), allBatchItems, aspectRetriever);
        subResources.addAll(domainUrns);
      }

      if (!AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
          session,
          ApiOperation.fromChangeType(item.getChangeType()),
          List.of(item.getUrn()),
          subResources)) {
        return List.of(
            AspectValidationException.forItem(
                item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
      }
    }
    return Collections.emptyList();
  }

  private List<AspectValidationException> validateEditableSchemaMetadata(
      AuthorizationSession session,
      BatchItem item,
      Collection<? extends BatchItem> allBatchItems,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentSchemaAspect) {
    EditableSchemaMetadata editableSchemaMetadata;
    EditableSchemaMetadata currentSchema =
        currentSchemaAspect == null
            ? null
            : RecordUtils.toRecordTemplate(
                EditableSchemaMetadata.class, currentSchemaAspect.data());
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof ProposedItem) {
      ProposedItem proposedItem = (ProposedItem) item;
      PatchItemImpl patchItem =
          PatchItemImpl.builder()
              .build(
                  proposedItem.getMetadataChangeProposal(),
                  proposedItem.getAuditStamp(),
                  aspectRetriever.getEntityRegistry());
      editableSchemaMetadata =
          patchItem
              .applyPatch(currentSchema, aspectRetriever)
              .getAspect(EditableSchemaMetadata.class);
    } else {
      editableSchemaMetadata = item.getAspect(EditableSchemaMetadata.class);
    }
    if (editableSchemaMetadata != null
        && editableSchemaMetadata.getEditableSchemaFieldInfo() != null) {
      final Map<String, GlobalTags> existingTagsMap = new HashMap<>();
      if (currentSchema != null && currentSchema.getEditableSchemaFieldInfo() != null) {
        existingTagsMap.putAll(
            currentSchema.getEditableSchemaFieldInfo().stream()
                .collect(
                    Collectors.toMap(
                        EditableSchemaFieldInfo::getFieldPath,
                        schemaField ->
                            Optional.ofNullable(schemaField.getGlobalTags())
                                .orElse(new GlobalTags()))));
      }
      Set<Urn> tagDifference =
          editableSchemaMetadata.getEditableSchemaFieldInfo().stream()
              .map(
                  schemaField ->
                      extractTagDifference(
                          Optional.ofNullable(schemaField.getGlobalTags()).orElse(new GlobalTags()),
                          existingTagsMap.get(schemaField.getFieldPath())))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      // Combine tags + domains (if enabled) as subResources for authorization check
      Set<Urn> subResources = new HashSet<>(tagDifference);

      // Only collect domain information if domain-based authorization is enabled
      if (domainBasedAuthorizationEnabled) {
        Set<Urn> domainUrns =
            getEntityDomainsFromBatchOrDB(item.getUrn(), allBatchItems, aspectRetriever);
        subResources.addAll(domainUrns);
      }

      if (!AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
          session,
          ApiOperation.fromChangeType(item.getChangeType()),
          List.of(item.getUrn()),
          subResources)) {
        return List.of(
            AspectValidationException.forItem(
                item,
                "Unauthorized to modify editable schema field tags on entity: " + item.getUrn()));
      }
    }
    return Collections.emptyList();
  }

  /**
   * Get the domain URNs for an entity to include as subResources in authorization checks. This
   * allows policies to filter based on both entity domains AND other subResources like tags.
   *
   * <p>The PolicyEngine is designed to handle heterogeneous subResources and has special logic to
   * extract domain information from the entire subResource collection when evaluating DOMAIN field
   * criteria.
   */
  private Set<Urn> getEntityDomains(Urn entityUrn, AspectRetriever aspectRetriever) {
    try {
      Aspect domainsAspect = aspectRetriever.getLatestAspectObject(entityUrn, DOMAINS_ASPECT_NAME);
      if (domainsAspect != null) {
        Domains domains = RecordUtils.toRecordTemplate(Domains.class, domainsAspect.data());
        return new HashSet<>(domains.getDomains());
      }
    } catch (Exception e) {
      log.warn("Failed to retrieve domains for entity {}: {}", entityUrn, e.getMessage());
    }
    return Collections.emptySet();
  }

  /**
   * Get domain URNs for an entity using UNION strategy.
   *
   * <p>UNION-BASED AUTHORIZATION: we must check permissions against ALL domains: - Existing domains
   * (from entity) and current domains - New domains (from batch)
   *
   * @param entityUrn the entity URN to get domains for
   * @param allBatchItems all items in the current batch
   * @param aspectRetriever for DB lookup of existing domains
   * @return set of domain URNs (union of existing + batch domains)
   */
  private Set<Urn> getEntityDomainsFromBatchOrDB(
      Urn entityUrn,
      Collection<? extends BatchItem> allBatchItems,
      AspectRetriever aspectRetriever) {

    Set<Urn> domainUnion = new HashSet<>(getEntityDomains(entityUrn, aspectRetriever));

    // Add domains from batch if entity is being updated
    allBatchItems.stream()
        .filter(
            item ->
                entityUrn.equals(item.getUrn()) && DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
        .forEach(
            item -> {
              try {
                Domains domains = item.getAspect(Domains.class);
                if (domains != null && domains.getDomains() != null) {
                  domainUnion.addAll(domains.getDomains());
                }
              } catch (Exception e) {
                log.warn("Failed to extract domains from batch item: {}", e.getMessage());
              }
            });

    return domainUnion;
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
