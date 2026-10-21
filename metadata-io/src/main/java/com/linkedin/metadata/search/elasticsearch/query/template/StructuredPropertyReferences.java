package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD_PREFIX;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A structured-property definition URN is a field reference. Inline text writes it as a Mustache
 * constant, {@code {{urn:li:structuredProperty:...}}}, which resolves to the field with no
 * subfield. A {@code .keyword} or {@code .text} suffix after that constant, or a {@code subfield}
 * member, is applied outside the URN. Both forms are legal on string and rich text.
 */
public final class StructuredPropertyReferences {
  public static final String KEYWORD = StructuredPropertySubfields.KEYWORD;
  public static final String TEXT = StructuredPropertySubfields.TEXT;
  public static final String PREFIX = "urn:li:structuredProperty:";
  private static final Pattern TAG =
      Pattern.compile("\\{\\{\\s*(" + Pattern.quote(PREFIX) + "[^{}]+?)\\s*\\}\\}");

  private StructuredPropertyReferences() {}

  public interface Resolver {
    @Nonnull
    String resolve(@Nonnull String field, @Nullable String subfield);

    @Nonnull
    default String resolveRemove(@Nonnull String field) {
      return resolve(field, null);
    }
  }

  @Nonnull
  public static Resolver resolver(
      @Nullable OperationFingerprint context, @Nullable AspectRetriever retriever) {
    return new Resolver() {
      @Override
      @Nonnull
      public String resolve(@Nonnull String field, @Nullable String subfield) {
        return resolveMember(field, subfield, context, retriever);
      }

      @Override
      @Nonnull
      public String resolveRemove(@Nonnull String field) {
        return StructuredPropertyReferences.resolveRemove(field, context, retriever);
      }
    };
  }

  @Nonnull
  public static Resolver omitting() {
    return new Resolver() {
      @Override
      @Nonnull
      public String resolve(@Nonnull String field, @Nullable String subfield) {
        if (field.startsWith(PREFIX)) {
          return "";
        }
        if (subfield != null && !subfield.isBlank()) {
          throw new SearchTemplateException(
              "Search template subfield is only valid on a structured property");
        }
        return field;
      }

      @Override
      @Nonnull
      public String resolveRemove(@Nonnull String field) {
        return field.startsWith(PREFIX) ? "" : field;
      }
    };
  }

  /**
   * Drops each structured-property constant and a following {@code .keyword} or {@code .text}. Used
   * when the template is served from the shipped file and definitions are treated as absent.
   */
  @Nullable
  public static String omitStored(@Nullable String text) {
    if (text == null || !text.contains(PREFIX)) {
      return text;
    }
    Matcher matcher = TAG.matcher(text);
    StringBuilder out = new StringBuilder();
    int cursor = 0;
    while (matcher.find()) {
      out.append(text, cursor, matcher.start());
      cursor = matcher.end();
      if (boundedSuffix(text, cursor, KEYWORD)) {
        cursor += KEYWORD.length() + 1;
      } else if (boundedSuffix(text, cursor, TEXT)) {
        cursor += TEXT.length() + 1;
      }
    }
    out.append(text.substring(cursor));
    return out.toString();
  }

  @Nonnull
  public static Resolver identity() {
    return (field, subfield) -> {
      if (subfield != null && !subfield.isBlank()) {
        throw new SearchTemplateException(
            "Search template subfield is only valid on a structured property");
      }
      return field;
    };
  }

  /**
   * Replaces each structured-property constant and leaves a following {@code .keyword} or {@code
   * .text} in place. Either suffix is rejected on a date, a number, or a URN.
   */
  @Nullable
  public static String rewrite(
      @Nullable String text,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (text == null || !text.contains(PREFIX)) {
      return text;
    }
    Matcher matcher = TAG.matcher(text);
    StringBuilder out = new StringBuilder();
    int cursor = 0;
    while (matcher.find()) {
      out.append(text, cursor, matcher.start());
      out.append(
          resolveUrn(matcher.group(1).trim(), formAfter(text, matcher.end()), context, retriever));
      cursor = matcher.end();
    }
    if (cursor == 0) {
      throw new SearchTemplateException(
          "Search template structured property reference is not a Mustache constant");
    }
    out.append(text.substring(cursor));
    return out.toString();
  }

  @Nonnull
  public static String resolveMember(
      @Nonnull String field,
      @Nullable String subfield,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (field.startsWith(STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD_PREFIX)) {
      throw new SearchTemplateException(
          "Search template structured property versioned path is not a logical name: " + field);
    }
    String form = form(subfield);
    if (field.startsWith(PREFIX)) {
      String path = resolveUrn(field, form, context, retriever);
      return form == null ? path : path + "." + form;
    }
    if (field.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + ".")) {
      return resolveIndexedOrLogical(field, form, context, retriever);
    }
    if (form != null) {
      throw new SearchTemplateException(
          "Search template subfield is only valid on a structured property");
    }
    return field;
  }

  @Nullable
  private static String formAfter(@Nonnull String text, int end) {
    if (boundedSuffix(text, end, KEYWORD)) {
      return KEYWORD;
    }
    if (boundedSuffix(text, end, TEXT)) {
      return TEXT;
    }
    return null;
  }

  private static boolean boundedSuffix(@Nonnull String text, int end, @Nonnull String form) {
    String token = "." + form;
    if (!text.startsWith(token, end)) {
      return false;
    }
    int after = end + token.length();
    if (after >= text.length()) {
      return true;
    }
    char next = text.charAt(after);
    return !Character.isLetterOrDigit(next) && next != '_';
  }

  @Nullable
  private static String form(@Nullable String subfield) {
    if (subfield == null || subfield.isBlank()) {
      return null;
    }
    if (KEYWORD.equals(subfield) || TEXT.equals(subfield)) {
      return subfield;
    }
    throw new SearchTemplateException("Search template subfield is not valid: " + subfield);
  }

  @Nonnull
  public static String resolveRemove(
      @Nonnull String field,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (field.startsWith(PREFIX)) {
      return resolveMember(field, null, context, retriever);
    }
    String rewritten = rewrite(field, context, retriever);
    if (rewritten != null
        && !rewritten.equals(field)
        && rewritten.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + ".")) {
      return rewritten;
    }
    return resolveMember(rewritten == null ? field : rewritten, null, context, retriever);
  }

  /**
   * Parent merge stores the indexed path. A later resolve of that path has no dotted qualified name
   * left to look up, so an unknown single-segment suffix is already the indexed field.
   */
  @Nonnull
  private static String resolveIndexedOrLogical(
      @Nonnull String field,
      @Nullable String form,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    String suffix = field.substring((STRUCTURED_PROPERTY_MAPPING_FIELD + ".").length());
    boolean known =
        !suffix.contains(".")
            && retriever != null
            && context != null
            && StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(
                    context, field, retriever)
                .isPresent();
    if (!suffix.contains(".") && !known) {
      if (form != null) {
        throw new SearchTemplateException(
            "Search template structured property could not be resolved: " + field);
      }
      return field;
    }
    return resolveLogical(field, form, context, retriever);
  }

  @Nonnull
  private static String resolveLogical(
      @Nonnull String field,
      @Nullable String form,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (retriever == null || context == null) {
      throw new SearchTemplateException(
          "Search template structured property could not be resolved: " + field);
    }
    var found =
        StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(context, field, retriever);
    if (found.isEmpty()) {
      throw new SearchTemplateException(
          "Search template structured property could not be resolved: " + field);
    }
    StructuredPropertyDefinition definition = found.get().getSecond();
    requireForm(form, StructuredPropertyUtils.getLogicalValueType(definition), field);
    String path =
        STRUCTURED_PROPERTY_MAPPING_FIELD
            + "."
            + StructuredPropertyUtils.toElasticsearchFieldName(found.get().getFirst(), definition);
    return form == null ? path : path + "." + form;
  }

  @Nonnull
  private static String resolveUrn(
      @Nonnull String raw,
      @Nullable String form,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (retriever == null || context == null) {
      throw new SearchTemplateException(
          "Search template structured property could not be resolved: " + raw);
    }
    Urn urn;
    try {
      urn = UrnUtils.getUrn(raw);
    } catch (RuntimeException e) {
      throw new SearchTemplateException(
          "Search template structured property could not be resolved: " + raw, e);
    }
    if (!"structuredProperty".equals(urn.getEntityType())) {
      throw new SearchTemplateException(
          "Search template structured property could not be resolved: " + raw);
    }
    Aspect aspect =
        retriever.getLatestAspectObject(context, urn, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    if (aspect == null || aspect.data() == null) {
      throw new SearchTemplateException(
          "Search template structured property could not be resolved: " + raw);
    }
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition(aspect.data());
    requireForm(form, StructuredPropertyUtils.getLogicalValueType(definition), raw);
    return STRUCTURED_PROPERTY_MAPPING_FIELD
        + "."
        + StructuredPropertyUtils.toElasticsearchFieldName(urn, definition);
  }

  private static void requireForm(
      @Nullable String form, @Nonnull LogicalValueType type, @Nonnull String field) {
    if (form == null) {
      return;
    }
    if (StructuredPropertyUtils.usesKeywordSubfield(type)) {
      return;
    }
    throw new SearchTemplateException(
        "Search template structured property has no " + form + " subfield: " + field);
  }
}
