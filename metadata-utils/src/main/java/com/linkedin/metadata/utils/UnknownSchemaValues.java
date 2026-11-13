package com.linkedin.metadata.utils;

import com.linkedin.data.message.Message;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Values a newer version added to its schema without bumping {@code @Aspect.schemaVersion}: enum
 * symbols and union members. Adding either is additive (see {@code bump_schema_versions.py}), so an
 * aspect that version wrote carries the same schema version as this one's, yet fails validation
 * here.
 *
 * <p>This version rejects such values on write, so stored data holding them was written by a newer
 * version (e.g. read after a rollback). An aspect that fails validation only because of them is
 * treated as from a newer version; any other validation error is a real error.
 */
public final class UnknownSchemaValues {

  // Pegasus ValidateDataAgainstSchema message formats.
  static final String UNKNOWN_ENUM_SYMBOL = "\"%1$s\" is not an enum symbol";
  static final String UNKNOWN_UNION_MEMBER = "\"%1$s\" is not a member type of union %2$s";

  private static final Set<String> UNKNOWN_VALUE_FORMATS =
      Set.of(UNKNOWN_ENUM_SYMBOL, UNKNOWN_UNION_MEMBER);

  /** True when the validation failed, and only on enum symbols or union members it doesn't know. */
  public static boolean failsOnlyOnUnknownValues(@Nullable final ValidationResult result) {
    if (result == null || result.isValid()) {
      return false;
    }
    final List<Message> errors =
        result.getMessages().stream().filter(Message::isError).collect(Collectors.toList());
    return !errors.isEmpty()
        && errors.stream().allMatch(m -> UNKNOWN_VALUE_FORMATS.contains(m.getFormat()));
  }

  /**
   * As {@link #failsOnlyOnUnknownValues(ValidationResult)}, validating the aspect the way it is
   * validated on read and write (unrecognized fields are trimmed, not errors).
   */
  public static boolean failsOnlyOnUnknownValues(@Nonnull final RecordTemplate aspect) {
    final AtomicReference<ValidationResult> failure = new AtomicReference<>();
    RecordTemplateValidator.validateTrim(copyOf(aspect), failure::set);
    return failsOnlyOnUnknownValues(failure.get());
  }

  /** Trim mode removes unrecognized fields; validate a copy so the caller's aspect is untouched. */
  @Nonnull
  private static RecordTemplate copyOf(@Nonnull final RecordTemplate aspect) {
    try {
      return aspect.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy aspect for validation", e);
    }
  }

  private UnknownSchemaValues() {}
}
