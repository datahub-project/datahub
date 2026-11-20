package com.linkedin.common;

import com.linkedin.data.message.Message;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.validator.ValidatorContext;
import com.linkedin.data.template.RecordTemplate;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Rules for environment names held in {@code FabricTypeName} fields: a built-in {@link FabricType}
 * symbol, or a custom name of the same shape. Whether a custom name is registered is checked
 * separately, against customFabricType entities.
 */
public final class FabricTypeNames {

  public static final String TYPEREF_FULL_NAME = "com.linkedin.common.FabricTypeName";

  private static final Pattern WELL_FORMED = Pattern.compile("^[A-Z][A-Z0-9_]{0,63}$");

  private static final Set<String> BUILT_IN =
      Arrays.stream(FabricType.values())
          .filter(value -> value != FabricType.$UNKNOWN)
          .map(Enum::name)
          .collect(Collectors.toUnmodifiableSet());

  private FabricTypeNames() {}

  public static boolean isBuiltIn(@Nullable String name) {
    return name != null && BUILT_IN.contains(name);
  }

  /** Uppercase letters, digits and underscores, starting with a letter, at most 64 characters. */
  public static boolean isWellFormed(@Nullable String name) {
    return name != null && WELL_FORMED.matcher(name).matches();
  }

  /** True when the name equals a built-in symbol ignoring case, so it can't be a custom name. */
  public static boolean collidesWithBuiltIn(@Nullable String name) {
    return name != null && BUILT_IN.stream().anyMatch(builtIn -> builtIn.equalsIgnoreCase(name));
  }

  public static boolean isFabricTypeName(@Nullable DataSchema schema) {
    return schema != null
        && DataSchema.Type.TYPEREF.equals(schema.getType())
        && TYPEREF_FULL_NAME.equals(((NamedDataSchema) schema).getFullName());
  }

  /**
   * The stored value of a {@code FabricTypeName} field. Generated accessors return {@code $UNKNOWN}
   * for a custom value, so code that needs the real value reads it here.
   */
  @Nullable
  public static String get(@Nonnull RecordTemplate record, @Nonnull String fieldName) {
    return record.data().getString(fieldName);
  }

  /** Writes any value, built-in or custom, to a {@code FabricTypeName} field. */
  public static void set(
      @Nonnull RecordTemplate record, @Nonnull String fieldName, @Nonnull String value) {
    record.data().put(fieldName, value);
  }

  /**
   * Pegasus validator hook: rejects a malformed value in any {@code FabricTypeName} field. This
   * restores, for every env / origin field and every URN key, the shape check the enum used to
   * give.
   */
  public static void validate(ValidatorContext context) {
    if (!isFabricTypeName(context.dataElement().getSchema())) {
      return;
    }
    Object value = context.dataElement().getValue();
    if (!(value instanceof String) || !isWellFormed((String) value)) {
      context.addResult(
          new Message(
              context.dataElement().path(),
              "\"%s\" is not a valid environment name: use uppercase letters, digits and"
                  + " underscores, starting with a letter",
              value));
      context.setHasFix(false);
    }
  }
}
