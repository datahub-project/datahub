package com.linkedin.metadata.models.annotation.v3;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import com.linkedin.metadata.models.annotation.AnnotationUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

/**
 * One Search V3 representation of a field value. The entry with no {@code subfield} is the exact
 * root. A subfield is an alternate representation indexed only on a shared label.
 */
@Value
public class FieldMapping {

  /** Word subfield. {@code SearchFields.TEXT} is this name. */
  public static final String TEXT = "text";

  /** Stemmed subfield. {@code SearchFields.STEMMED} is this name. */
  public static final String STEMMED = "stemmed";

  public enum Type {
    KEYWORD,
    TEXT,
    STEMMED,
    TEXT_PARTIAL,
    WORD_GRAM_2,
    URN,
    BOOLEAN,
    COUNT,
    DATETIME,
    DOUBLE,
    BROWSE_PATH_V2,
    OBJECT
  }

  /** Null when this entry is the root. */
  @Nullable String subfield;

  @Nonnull Type type;

  /** Omitted in the annotation means true, and that default is part of the root identity. */
  boolean index;

  boolean docValues;

  public boolean isRoot() {
    return subfield == null;
  }

  @Nonnull
  public static FieldMapping root(@Nonnull Type type) {
    return new FieldMapping(null, type, true, true);
  }

  @Nonnull
  public static FieldMapping root(@Nonnull Type type, boolean index, boolean docValues) {
    return new FieldMapping(null, type, index, docValues);
  }

  @Nonnull
  public static FieldMapping subfield(@Nonnull String name, @Nonnull Type type) {
    boolean analyzer = isAnalyzerType(type);
    return new FieldMapping(name, type, true, !analyzer);
  }

  public boolean sameRoot(@Nonnull FieldMapping other) {
    return isRoot()
        && other.isRoot()
        && type == other.type
        && index == other.index
        && docValues == other.docValues;
  }

  public boolean sameSubfield(@Nonnull FieldMapping other) {
    return !isRoot() && !other.isRoot() && subfield.equals(other.subfield) && type == other.type;
  }

  /** Whether this representation can index a value of the field's PDL type. */
  public boolean accepts(@Nonnull DataSchema.Type schemaType) {
    switch (type) {
      case KEYWORD:
      case TEXT:
      case STEMMED:
      case TEXT_PARTIAL:
      case WORD_GRAM_2:
      case URN:
      case BROWSE_PATH_V2:
        return schemaType == DataSchema.Type.STRING
            || schemaType == DataSchema.Type.ENUM
            || schemaType == DataSchema.Type.ARRAY
            || (type == Type.KEYWORD && schemaType == DataSchema.Type.MAP);
      case BOOLEAN:
        return schemaType == DataSchema.Type.BOOLEAN;
      case COUNT:
        return schemaType == DataSchema.Type.INT || schemaType == DataSchema.Type.LONG;
      case DATETIME:
        return schemaType == DataSchema.Type.LONG;
      case DOUBLE:
        return schemaType == DataSchema.Type.FLOAT || schemaType == DataSchema.Type.DOUBLE;
      case OBJECT:
        return schemaType == DataSchema.Type.RECORD || schemaType == DataSchema.Type.MAP;
      default:
        return false;
    }
  }

  /**
   * The root and label subfields V3 uses when {@code v3.mappings} is omitted. Analyzer subfields
   * are added only when the field has a search destination; {@code _aspects} does not use this.
   */
  @Nonnull
  public static List<FieldMapping> fallback(
      @Nonnull FieldType fieldType, boolean hasSearchDestination) {
    List<FieldMapping> mappings = new ArrayList<>();
    mappings.add(root(rootType(fieldType)));
    if (hasSearchDestination && hasAnalyzerFallback(fieldType)) {
      mappings.add(subfield(TEXT, Type.TEXT));
      mappings.add(subfield(STEMMED, Type.STEMMED));
    }
    return List.copyOf(mappings);
  }

  /**
   * Parses {@code v3.mappings}. Explicit entries replace {@link #fallback}, they do not merge with
   * it. Exactly one root is required.
   */
  @Nonnull
  public static List<FieldMapping> parse(@Nonnull List<?> raw, @Nonnull String context) {
    if (raw.isEmpty()) {
      throw invalid(context, "mappings cannot be empty");
    }
    List<FieldMapping> mappings = new ArrayList<>();
    int roots = 0;
    for (Object item : raw) {
      if (!(item instanceof Map)) {
        throw invalid(context, "each mappings entry must be an object");
      }
      @SuppressWarnings("rawtypes")
      Map entry = (Map) item;
      Optional<String> typeName = AnnotationUtils.getField(entry, "type", String.class);
      if (typeName.isEmpty() || !EnumUtils.isValidEnum(Type.class, typeName.get())) {
        throw invalid(context, "mappings entry has missing or unknown type");
      }
      Type type = Type.valueOf(typeName.get());
      Optional<String> subfield = AnnotationUtils.getField(entry, "subfield", String.class);
      if (subfield.isEmpty() || subfield.get().isEmpty()) {
        if (isAnalyzerType(type)) {
          throw invalid(context, "an analyzer type cannot be the root; it belongs on a subfield");
        }
        boolean index = AnnotationUtils.getField(entry, "index", Boolean.class).orElse(true);
        boolean docValues =
            AnnotationUtils.getField(entry, "docValues", Boolean.class).orElse(true);
        roots++;
        mappings.add(new FieldMapping(null, type, index, docValues));
      } else {
        if (!isSubfieldType(type)) {
          throw invalid(
              context, "subfield '" + subfield.get() + "' has a type that is not an alternate");
        }
        validateSubfieldName(subfield.get(), context);
        mappings.add(subfield(subfield.get(), type));
      }
    }
    if (roots != 1) {
      throw invalid(context, "mappings must contain exactly one root entry");
    }
    FieldMapping root = rootOf(mappings);
    boolean hasSubfield = mappings.stream().anyMatch(mapping -> !mapping.isRoot());
    if (hasSubfield && root.getType() != Type.KEYWORD && root.getType() != Type.URN) {
      throw invalid(context, "only a string root can have subfields");
    }
    Map<String, Type> seen = new LinkedHashMap<>();
    for (FieldMapping mapping : mappings) {
      if (mapping.isRoot()) {
        continue;
      }
      Type previous = seen.put(mapping.getSubfield(), mapping.getType());
      if (previous != null) {
        throw invalid(context, "subfield '" + mapping.getSubfield() + "' is declared twice");
      }
    }
    return List.copyOf(mappings);
  }

  /**
   * Mappings stored on the annotation, or the field-type fallback when a caller has not parsed an
   * annotation (tests that mock one).
   */
  @Nonnull
  public static List<FieldMapping> of(@Nonnull SearchableAnnotation annotation) {
    SearchableAnnotation resolved = annotation.resolvedForV3();
    if (resolved == null) {
      resolved = annotation;
    }
    List<FieldMapping> mappings = resolved.getV3Mappings();
    if (mappings != null && !mappings.isEmpty()) {
      return mappings;
    }
    FieldType fieldType = annotation.getFieldType();
    return fallback(fieldType == null ? FieldType.KEYWORD : fieldType, false);
  }

  @Nonnull
  public static FieldMapping rootOf(@Nonnull List<FieldMapping> mappings) {
    return mappings.stream()
        .filter(FieldMapping::isRoot)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("mappings have no root"));
  }

  /**
   * One root plus every contributor's subfields. Identical subfields collapse. A different root or
   * a subfield declared twice with different types fails.
   */
  @Nonnull
  public static Resolved union(
      @Nonnull String label, @Nonnull List<List<FieldMapping>> contributors) {
    FieldMapping root = null;
    Map<String, FieldMapping> subfields = new LinkedHashMap<>();
    for (List<FieldMapping> mappings : contributors) {
      FieldMapping contributorRoot = rootOf(mappings);
      if (root == null) {
        root = contributorRoot;
      } else if (!root.sameRoot(contributorRoot)) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: searchLabel '%s' contributors must resolve to"
                    + " the same root. Found %s (index=%s, docValues=%s) and %s (index=%s,"
                    + " docValues=%s)",
                SearchableAnnotation.ANNOTATION_NAME,
                label,
                root.getType(),
                root.isIndex(),
                root.isDocValues(),
                contributorRoot.getType(),
                contributorRoot.isIndex(),
                contributorRoot.isDocValues()));
      }
      for (FieldMapping mapping : mappings) {
        if (mapping.isRoot()) {
          continue;
        }
        FieldMapping existing = subfields.putIfAbsent(mapping.getSubfield(), mapping);
        if (existing != null && !existing.sameSubfield(mapping)) {
          throw new ModelValidationException(
              String.format(
                  "Failed to validate @%s annotations: searchLabel '%s' subfield '%s' is declared"
                      + " as %s and %s",
                  SearchableAnnotation.ANNOTATION_NAME,
                  label,
                  mapping.getSubfield(),
                  existing.getType(),
                  mapping.getType()));
        }
      }
    }
    if (root == null) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotations: searchLabel '%s' has no resolved root",
              SearchableAnnotation.ANNOTATION_NAME, label));
    }
    return new Resolved(root, List.copyOf(subfields.values()));
  }

  private static Type rootType(@Nonnull FieldType fieldType) {
    switch (fieldType) {
      case DATETIME:
        return Type.DATETIME;
      case BOOLEAN:
        return Type.BOOLEAN;
      case COUNT:
        return Type.COUNT;
      case DOUBLE:
        return Type.DOUBLE;
      case OBJECT:
      case MAP_ARRAY:
        return Type.OBJECT;
      case BROWSE_PATH:
        throw new ModelValidationException(
            "Failed to validate @"
                + SearchableAnnotation.ANNOTATION_NAME
                + " annotation: field type BROWSE_PATH is not supported on Search V3");
      case BROWSE_PATH_V2:
        return Type.BROWSE_PATH_V2;
      case KEYWORD:
      case TEXT:
      case TEXT_PARTIAL:
      case WORD_GRAM:
      case URN:
      case URN_PARTIAL:
      default:
        return Type.KEYWORD;
    }
  }

  private static boolean hasAnalyzerFallback(@Nonnull FieldType fieldType) {
    return fieldType == FieldType.TEXT
        || fieldType == FieldType.TEXT_PARTIAL
        || fieldType == FieldType.WORD_GRAM
        || fieldType == FieldType.URN
        || fieldType == FieldType.URN_PARTIAL;
  }

  private static boolean isAnalyzerType(@Nonnull Type type) {
    return type == Type.TEXT
        || type == Type.STEMMED
        || type == Type.TEXT_PARTIAL
        || type == Type.WORD_GRAM_2;
  }

  private static boolean isSubfieldType(@Nonnull Type type) {
    return isAnalyzerType(type) || type == Type.KEYWORD || type == Type.URN;
  }

  private static void validateSubfieldName(@Nonnull String name, @Nonnull String context) {
    if (!name.matches("^[a-zA-Z][a-zA-Z0-9]*$")) {
      throw invalid(context, "subfield '" + name + "' is not a valid field name");
    }
  }

  private static ModelValidationException invalid(@Nonnull String context, @Nonnull String detail) {
    return new ModelValidationException(
        String.format(
            "Failed to validate @%s annotation declared at %s: %s",
            SearchableAnnotation.ANNOTATION_NAME, context, detail));
  }

  /** The mapping of one shared label after its contributors are combined. */
  @Value
  public static class Resolved {
    @Nonnull FieldMapping root;
    @Nonnull List<FieldMapping> subfields;
  }
}
