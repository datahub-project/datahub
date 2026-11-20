package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.google.common.collect.Streams;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(chain = true)
public class TagPrivilegeConstraintsValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    List<AspectValidationException> failures = new ArrayList<>();

    Map<String, Map<Urn, Aspect>> currentAspects =
        fetchCurrentAspects(operationContext, items, aspectRetriever);
    Map<Urn, Boolean> entityExists = entityExists(operationContext, items, aspectRetriever);

    for (BatchItem item : items) {
      Aspect currentAspect =
          currentAspects
              .getOrDefault(item.getAspectName(), Collections.emptyMap())
              .get(item.getUrn());
      // A missing answer is not "does not exist": that would classify the write as a create and
      // let Create Entity authorize a change to an entity that may already exist.
      if (checksTags(item.getAspectName()) && existenceUnknown(item, entityExists)) {
        failures.add(
            AspectValidationException.forAuth(
                item, "Cannot authorize a tag write without an existence result"));
        continue;
      }
      switch (item.getAspectName()) {
        case GLOBAL_TAGS_ASPECT_NAME:
          failures.addAll(
              validateGlobalTags(
                  session,
                  item,
                  aspectRetriever,
                  currentAspect,
                  operationFor(item.getUrn(), item.getChangeType(), entityExists)));
          break;
        case SCHEMA_METADATA_ASPECT_NAME:
          failures.addAll(
              validateSchemaMetadata(
                  session,
                  item,
                  aspectRetriever,
                  currentAspect,
                  operationFor(item.getUrn(), item.getChangeType(), entityExists)));
          break;
        case EDITABLE_SCHEMA_METADATA_ASPECT_NAME:
          failures.addAll(
              validateEditableSchemaMetadata(
                  session,
                  item,
                  aspectRetriever,
                  currentAspect,
                  operationFor(item.getUrn(), item.getChangeType(), entityExists)));
          break;
        default:
          break;
      }
    }
    return failures;
  }

  @Nonnull
  private Map<String, Map<Urn, Aspect>> fetchCurrentAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull AspectRetriever aspectRetriever) {
    Map<String, Set<Urn>> urnsByAspectName = new HashMap<>();
    for (BatchItem item : items) {
      switch (item.getAspectName()) {
        case GLOBAL_TAGS_ASPECT_NAME:
        case SCHEMA_METADATA_ASPECT_NAME:
        case EDITABLE_SCHEMA_METADATA_ASPECT_NAME:
          urnsByAspectName
              .computeIfAbsent(item.getAspectName(), name -> new HashSet<>())
              .add(item.getUrn());
          break;
        default:
          break;
      }
    }

    Map<String, Map<Urn, Aspect>> currentAspects = new HashMap<>();
    urnsByAspectName.forEach(
        (aspectName, urns) -> {
          Map<Urn, Aspect> byUrn = new HashMap<>();
          AspectRetriever.getLatestAspectObjectsAcrossEntityTypes(
                  aspectRetriever, operationContext, urns, Set.of(aspectName))
              .forEach(
                  (urn, aspectsByName) -> {
                    Aspect aspect = aspectsByName.get(aspectName);
                    if (aspect != null) {
                      byUrn.put(urn, aspect);
                    }
                  });
          currentAspects.put(aspectName, byUrn);
        });
    return currentAspects;
  }

  private List<AspectValidationException> validateGlobalTags(
      AuthorizationSession session,
      BatchItem item,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentTagsAspect,
      @Nonnull Supplier<ApiOperation> operation) {
    GlobalTags currentTags =
        currentTagsAspect == null
            ? null
            : RecordUtils.toRecordTemplate(GlobalTags.class, currentTagsAspect.data());
    final Set<Urn> tagDifference;
    if (ChangeType.DELETE.equals(item.getChangeType())) {
      // The whole aspect is being removed, so every current tag is removed. Each one must be
      // authorized, or a constrained user could bypass the per-tag check by deleting the aspect.
      tagDifference =
          currentTags == null
              ? Collections.emptySet()
              : extractTagDifference(new GlobalTags(), currentTags);
    } else {
      GlobalTags newTags =
          PatchItemImpl.resolveProposedAspect(item, currentTags, aspectRetriever, GlobalTags.class);
      if (newTags == null) {
        return Collections.emptyList();
      }
      tagDifference = extractTagDifference(newTags, currentTags);
    }
    if (!tagDifference.isEmpty()
        && !AuthUtil.isAPIAuthorizedForTagModification(
            session,
            item.getUrn(),
            tagDifference,
            PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE,
            operation.get())) {
      return List.of(
          AspectValidationException.forAuth(
              item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
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
    if (currentTags != null) {
      Set<Urn> existingTagUrns =
          Optional.ofNullable(currentTags.getTags(GetMode.NULL))
              .orElse(new TagAssociationArray())
              .stream()
              .map(TagAssociation::getTag)
              .collect(Collectors.toSet());
      return Streams.concat(
              existingTagUrns.stream().filter(urn -> !tagUrns.contains(urn)),
              tagUrns.stream().filter(urn -> !existingTagUrns.contains(urn)))
          .collect(Collectors.toSet());
    }
    return tagUrns;
  }

  private List<AspectValidationException> validateSchemaMetadata(
      AuthorizationSession session,
      BatchItem item,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentSchemaAspect,
      @Nonnull Supplier<ApiOperation> operation) {
    SchemaMetadata schemaMetadata;
    SchemaMetadata currentSchema =
        currentSchemaAspect == null
            ? null
            : RecordUtils.toRecordTemplate(SchemaMetadata.class, currentSchemaAspect.data());
    schemaMetadata =
        PatchItemImpl.resolveProposedAspect(
            item, currentSchema, aspectRetriever, SchemaMetadata.class);
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
      if (!tagDifference.isEmpty()
          && !AuthUtil.isAPIAuthorizedForTagModification(
              session,
              item.getUrn(),
              tagDifference,
              AuthUtil.tagModificationPrivilege(item.getUrn(), true),
              operation.get())) {
        return List.of(
            AspectValidationException.forAuth(
                item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
      }
    }
    return Collections.emptyList();
  }

  private List<AspectValidationException> validateEditableSchemaMetadata(
      AuthorizationSession session,
      BatchItem item,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentSchemaAspect,
      @Nonnull Supplier<ApiOperation> operation) {
    EditableSchemaMetadata editableSchemaMetadata;
    EditableSchemaMetadata currentSchema =
        currentSchemaAspect == null
            ? null
            : RecordUtils.toRecordTemplate(
                EditableSchemaMetadata.class, currentSchemaAspect.data());
    editableSchemaMetadata =
        PatchItemImpl.resolveProposedAspect(
            item, currentSchema, aspectRetriever, EditableSchemaMetadata.class);
    if (editableSchemaMetadata != null) {
      final Map<String, GlobalTags> existingTagsMap = new HashMap<>();
      if (currentSchema != null) {
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
      if (!tagDifference.isEmpty()
          && !AuthUtil.isAPIAuthorizedForTagModification(
              session,
              item.getUrn(),
              tagDifference,
              AuthUtil.tagModificationPrivilege(item.getUrn(), true),
              operation.get())) {
        return List.of(
            AspectValidationException.forAuth(
                item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
      }
    }
    return Collections.emptyList();
  }

  /**
   * One existence lookup for the batch. Schema fields, deletes, and aspect-level creates are left
   * out because their operation does not depend on existence.
   */
  @Nullable
  private static Map<Urn, Boolean> entityExists(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull AspectRetriever aspectRetriever) {
    Set<Urn> urns =
        items.stream()
            .filter(
                item ->
                    AuthUtil.subResourceWriteOperationNeedsExistence(
                        item.getChangeType(), item.getUrn()))
            .map(BatchItem::getUrn)
            .collect(Collectors.toSet());
    if (urns.isEmpty()) {
      return Map.of();
    }
    return aspectRetriever.entityExists(operationContext, urns);
  }

  private static boolean checksTags(@Nullable String aspectName) {
    return GLOBAL_TAGS_ASPECT_NAME.equals(aspectName)
        || SCHEMA_METADATA_ASPECT_NAME.equals(aspectName)
        || EDITABLE_SCHEMA_METADATA_ASPECT_NAME.equals(aspectName);
  }

  /**
   * True when the operation depends on existence and the lookup did not answer for this URN. An
   * explicit {@code false} is an answer; a null map, a missing key, or a null value is not.
   */
  private static boolean existenceUnknown(
      @Nonnull BatchItem item, @Nullable Map<Urn, Boolean> entityExists) {
    if (!AuthUtil.subResourceWriteOperationNeedsExistence(item.getChangeType(), item.getUrn())) {
      return false;
    }
    return entityExists == null || entityExists.get(item.getUrn()) == null;
  }

  /** Resolved only after a non-empty tag change. The existence map is already loaded. */
  private static Supplier<ApiOperation> operationFor(
      @Nonnull Urn urn, @Nonnull ChangeType changeType, @Nonnull Map<Urn, Boolean> entityExists) {
    return () ->
        AuthUtil.subResourceWriteOperation(
            changeType, urn, Boolean.TRUE.equals(entityExists.get(urn)));
  }
}
