package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
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
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
public class PolicyConstraintsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspectsWithAuth(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    if (session == null) {
      exceptions.addException(
          mcpItems.stream().findFirst().orElseThrow(IllegalStateException::new),
          "No authentication details found, cannot authorize change.");
      return exceptions.streamAllExceptions();
    }

    for (BatchItem item : mcpItems) {
      if (item.getSystemMetadata() != null
          && item.getSystemMetadata().getProperties() != null
          && UI_SOURCE.equals(item.getSystemMetadata().getProperties().get(APP_SOURCE))) {
        // We skip UI events, these are handled by LabelUtils.
        continue;
      }
      switch (item.getAspectName()) {
        case GLOBAL_TAGS_ASPECT_NAME:
          validateGlobalTags(
                  session,
                  item,
                  item.getAspect(GlobalTags.class),
                  retrieverContext
                      .getAspectRetriever()
                      .getLatestAspectObject(item.getUrn(), GLOBAL_TAGS_ASPECT_NAME))
              .forEach(exceptions::addException);
          break;
        case SCHEMA_METADATA_ASPECT_NAME:
          validateSchemaMetadata(
                  session,
                  item,
                  item.getAspect(SchemaMetadata.class),
                  retrieverContext
                      .getAspectRetriever()
                      .getLatestAspectObject(item.getUrn(), SCHEMA_METADATA_ASPECT_NAME))
              .forEach(exceptions::addException);
          break;
        case EDITABLE_SCHEMA_METADATA_ASPECT_NAME:
          validateEditableSchemaMetadata(
                  session,
                  item,
                  item.getAspect(EditableSchemaMetadata.class),
                  retrieverContext
                      .getAspectRetriever()
                      .getLatestAspectObject(item.getUrn(), EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
              .forEach(exceptions::addException);
          break;
        default:
          log.warn("Triggered PolicyConstraints Validator for unsupported aspect, ignoring.");
      }
    }

    return exceptions.streamAllExceptions();
  }

  private List<AspectValidationException> validateGlobalTags(
      AuthorizationSession session,
      BatchItem item,
      GlobalTags newTags,
      @Nullable Aspect currentTags) {
    if (newTags != null) {
      Set<Urn> tagDifference = extractTagDifference(newTags, currentTags);
      if (!AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
          session,
          ApiOperation.fromChangeType(item.getChangeType()),
          List.of(item.getUrn()),
          tagDifference)) {
        return List.of(
            AspectValidationException.forItem(
                item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
      }
    }
    return Collections.emptyList();
  }

  private Set<Urn> extractTagDifference(GlobalTags newTags, @Nullable Aspect currentTags) {
    if (currentTags == null) {
      return extractTagDifference(newTags, (GlobalTags) null);
    } else {
      GlobalTags existingTags = RecordUtils.toRecordTemplate(GlobalTags.class, currentTags.data());
      return extractTagDifference(newTags, existingTags);
    }
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
      SchemaMetadata schemaMetadata,
      @Nullable Aspect currentSchema) {
    if (schemaMetadata != null) {
      SchemaMetadata existingSchemaMetadata = null;
      final Map<String, GlobalTags> existingTagsMap = new HashMap<>();
      if (currentSchema != null) {
        existingSchemaMetadata =
            RecordUtils.toRecordTemplate(SchemaMetadata.class, currentSchema.data());
        existingTagsMap.putAll(
            existingSchemaMetadata.getFields().stream()
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
      if (!AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
          session,
          ApiOperation.fromChangeType(item.getChangeType()),
          List.of(item.getUrn()),
          tagDifference)) {
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
      EditableSchemaMetadata editableSchemaMetadata,
      @Nullable Aspect currentSchema) {
    if (editableSchemaMetadata != null) {
      EditableSchemaMetadata existingSchemaMetadata = null;
      final Map<String, GlobalTags> existingTagsMap = new HashMap<>();
      if (currentSchema != null) {
        existingSchemaMetadata =
            RecordUtils.toRecordTemplate(EditableSchemaMetadata.class, currentSchema.data());
        existingTagsMap.putAll(
            existingSchemaMetadata.getEditableSchemaFieldInfo().stream()
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
      if (!AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
          session,
          ApiOperation.fromChangeType(item.getChangeType()),
          List.of(item.getUrn()),
          tagDifference)) {
        return List.of(
            AspectValidationException.forItem(
                item, "Unauthorized to modify one or more tag Urns: " + tagDifference));
      }
    }
    return Collections.emptyList();
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
