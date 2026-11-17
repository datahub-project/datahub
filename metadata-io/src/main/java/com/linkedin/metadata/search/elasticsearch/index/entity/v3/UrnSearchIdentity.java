package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Full-text identity of a URN. The stored value stays the complete URN. Dataset names omit platform
 * and fabric. A schema field contributes its field path and not its parent dataset.
 */
public final class UrnSearchIdentity {

  private static final Pattern SCHEMA_ANNOTATION =
      Pattern.compile("\\[(?:version|type)=[^\\]]*\\]\\.?", Pattern.CASE_INSENSITIVE);
  private static final String ENCODED_UNIT_SEPARATOR = "%E2%90%9F";

  private UrnSearchIdentity() {}

  /** Parsed identity, or empty when the URN cannot be reduced to one. */
  @Nonnull
  public static String identity(@Nonnull Urn urn, @Nonnull EntityRegistry entityRegistry) {
    return identity(urn, entityRegistry, new HashSet<>());
  }

  /**
   * Decodes the reserved URN escapes {@code %28}, {@code %29}, {@code %2C}, and the encoded unit
   * separator. A literal {@code %} is left in place.
   */
  @Nonnull
  public static String decodeReserved(@Nonnull String value) {
    StringBuilder decoded = new StringBuilder(value.length());
    for (int index = 0; index < value.length(); index++) {
      if (value.regionMatches(
          true, index, ENCODED_UNIT_SEPARATOR, 0, ENCODED_UNIT_SEPARATOR.length())) {
        decoded.append('\u241F');
        index += ENCODED_UNIT_SEPARATOR.length() - 1;
        continue;
      }
      if (value.charAt(index) == '%' && index + 2 < value.length()) {
        char replacement = reservedReplacement(value.substring(index + 1, index + 3));
        if (replacement != 0) {
          decoded.append(replacement);
          index += 2;
          continue;
        }
      }
      decoded.append(value.charAt(index));
    }
    return decoded.toString();
  }

  /** Removes schema-field {@code [version=...]} and {@code [type=...]} annotations. */
  @Nonnull
  public static String cleanFieldPath(@Nonnull String fieldPath) {
    String cleaned = SCHEMA_ANNOTATION.matcher(fieldPath).replaceAll("");
    cleaned = cleaned.replaceAll("^\\.+|\\.+$", "");
    return cleaned.replaceAll("\\.{2,}", ".");
  }

  @Nonnull
  private static String identity(
      @Nonnull Urn urn, @Nonnull EntityRegistry entityRegistry, @Nonnull Set<String> seen) {
    if (!seen.add(urn.toString())) {
      return "";
    }
    final EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
    } catch (RuntimeException exception) {
      return "";
    }
    final RecordTemplate key;
    try {
      key = EntityKeyUtils.convertUrnToEntityKey(urn, entitySpec.getKeyAspectSpec());
    } catch (RuntimeException exception) {
      return identityFromDecodedStructure(urn, entityRegistry, seen);
    }
    if (Constants.DATASET_ENTITY_NAME.equals(urn.getEntityType())) {
      return reduceIfUrn(
          decodeReserved(new DatasetKey(key.data()).getName()), entityRegistry, seen);
    }
    if (Constants.SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())) {
      return cleanFieldPath(decodeReserved(new SchemaFieldKey(key.data()).getFieldPath()));
    }
    List<String> parts = new ArrayList<>();
    DataMap data = key.data();
    for (RecordDataSchema.Field field : key.schema().getFields()) {
      Object value = data.get(field.getName());
      if (value == null) {
        continue;
      }
      String part = reduceIfUrn(decodeReserved(String.valueOf(value)), entityRegistry, seen);
      if (!part.isEmpty()) {
        parts.add(part);
      }
    }
    return String.join(" ", parts);
  }

  /**
   * Reserved escapes can hide tuple structure ({@code %28} {@code %29} {@code %2C}). Decode those
   * and parse again. A literal percent is unchanged, so this is not a general URL decode.
   */
  @Nonnull
  private static String identityFromDecodedStructure(
      @Nonnull Urn urn, @Nonnull EntityRegistry entityRegistry, @Nonnull Set<String> seen) {
    String decoded = decodeReserved(urn.toString());
    if (decoded.equals(urn.toString())) {
      return "";
    }
    try {
      return identity(Urn.createFromString(decoded), entityRegistry, seen);
    } catch (URISyntaxException exception) {
      return "";
    }
  }

  @Nonnull
  private static String reduceIfUrn(
      @Nonnull String value, @Nonnull EntityRegistry entityRegistry, @Nonnull Set<String> seen) {
    if (!value.startsWith("urn:")) {
      return value;
    }
    try {
      String reduced = identity(Urn.createFromString(value), entityRegistry, seen);
      return reduced.isEmpty() ? value : reduced;
    } catch (URISyntaxException exception) {
      return value;
    }
  }

  private static char reservedReplacement(@Nullable String hex) {
    if (hex == null) {
      return 0;
    }
    if (hex.equalsIgnoreCase("28")) {
      return '(';
    }
    if (hex.equalsIgnoreCase("29")) {
      return ')';
    }
    if (hex.equalsIgnoreCase("2C")) {
      return ',';
    }
    return 0;
  }
}
