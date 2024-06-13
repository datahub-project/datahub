package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class FieldPathMutator extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {

    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();

    for (ChangeMCP item : changeMCPS) {
      if (changeTypeFilter(item) && aspectFilter(item)) {
        if (item.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)) {
          results.add(Pair.of(item, processSchemaMetadataAspect(item)));
        } else {
          results.add(Pair.of(item, processEditableSchemaMetadataAspect(item)));
        }
      } else {
        // no op
        results.add(Pair.of(item, false));
      }
    }

    return results.stream();
  }

  /*
    TODO: After some time, this should no longer be required. Assuming at least 1 write has
          occurred for all schema aspects.
  */
  @Override
  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {
    List<Pair<ReadItem, Boolean>> results = new LinkedList<>();

    for (ReadItem item : items) {
      if (aspectFilter(item)) {
        if (item.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)) {
          results.add(Pair.of(item, processSchemaMetadataAspect(item)));
        } else {
          results.add(Pair.of(item, processEditableSchemaMetadataAspect(item)));
        }
      } else {
        // no op
        results.add(Pair.of(item, false));
      }
    }

    return results.stream();
  }

  private static boolean changeTypeFilter(BatchItem item) {
    return !ChangeType.DELETE.equals(item.getChangeType())
        && !ChangeType.PATCH.equals(item.getChangeType());
  }

  private static boolean aspectFilter(ReadItem item) {
    return item.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)
        || item.getAspectName().equals(EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
  }

  private static boolean processEditableSchemaMetadataAspect(ReadItem item) {
    boolean mutated = false;
    final EditableSchemaMetadata schemaMetadata = item.getAspect(EditableSchemaMetadata.class);
    EditableSchemaFieldInfoArray fields = schemaMetadata.getEditableSchemaFieldInfo();
    List<EditableSchemaFieldInfo> replaceFields =
        deduplicateFieldPaths(fields, EditableSchemaFieldInfo::getFieldPath);
    if (!replaceFields.isEmpty()) {
      schemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray(replaceFields));
      mutated = true;
    }
    return mutated;
  }

  private static boolean processSchemaMetadataAspect(ReadItem item) {
    boolean mutated = false;
    final SchemaMetadata schemaMetadata = item.getAspect(SchemaMetadata.class);
    SchemaFieldArray fields = schemaMetadata.getFields();
    List<SchemaField> replaceFields = deduplicateFieldPaths(fields, SchemaField::getFieldPath);
    if (!replaceFields.isEmpty()) {
      schemaMetadata.setFields(new SchemaFieldArray(replaceFields));
      mutated = true;
    }
    return mutated;
  }

  private static <T> List<T> deduplicateFieldPaths(
      Collection<T> fields, Function<T, String> fieldPathExtractor) {

    // preserve order
    final LinkedHashMap<String, List<T>> grouped =
        fields.stream()
            .collect(
                Collectors.groupingBy(fieldPathExtractor, LinkedHashMap::new, Collectors.toList()));

    if (grouped.values().stream().anyMatch(v -> v.size() > 1)) {
      log.warn(
          "Duplicate field path(s) detected. Dropping duplicates: {}",
          grouped.values().stream().filter(v -> v.size() > 1).collect(Collectors.toList()));
      // return first
      return grouped.values().stream().map(l -> l.get(0)).collect(Collectors.toList());
    }

    return Collections.emptyList();
  }
}
