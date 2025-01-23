package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class SchemaFieldUtils {
  private static final String FIELD_PATH_V2 = "[version=2.0]";

  private SchemaFieldUtils() {}

  /**
   * Generate schemaFieldUrn from the parent dataset urn from schemaMetadata field
   *
   * @param parentUrn the dataset's urn
   * @param field field from schemaMetadata
   * @return schemaField URN
   */
  public static Urn generateSchemaFieldUrn(Urn parentUrn, SchemaField field) {
    return generateSchemaFieldUrn(parentUrn, field.getFieldPath());
  }

  public static String generateDocumentId(Urn schemaFieldUrn) {
    if (!SCHEMA_FIELD_ENTITY_NAME.equals(schemaFieldUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid URN, expected entity %s, found %s",
              SCHEMA_FIELD_ENTITY_NAME, schemaFieldUrn.getEntityType()));
    }

    return String.format(
        "urn:li:%s:(%s,%s)",
        SCHEMA_FIELD_ENTITY_NAME,
        sha256Hex(schemaFieldUrn.getEntityKey().get(0)),
        sha256Hex(schemaFieldUrn.getEntityKey().get(1)));
  }

  /**
   * Handles our url encoding for URN characters.
   *
   * @param parentUrn the dataset's urn
   * @param fieldPath field path
   * @return schemaField URN
   */
  public static Urn generateSchemaFieldUrn(
      @Nonnull final Urn parentUrn, @Nonnull final String fieldPath) {
    // we rely on schemaField fieldPaths to be encoded since we do that on the ingestion side
    final String encodedFieldPath =
        fieldPath.replaceAll("\\(", "%28").replaceAll("\\)", "%29").replaceAll(",", "%2C");
    return Urn.createFromTuple(SCHEMA_FIELD_ENTITY_NAME, parentUrn.toString(), encodedFieldPath);
  }

  /**
   * Produce schemaField URN aliases v1/v2 variants given a schemeField target URN (either v1/v2),
   * the parent dataset URN and the parent dataset's schemaMetadata aspect.
   *
   * @param datasetUrn parent dataset's urn
   * @param schemaMetadata parent dataset's schemaMetadata
   * @param schemaField target schemeField
   * @return all aliases of the given schemaField URN including itself (if it exists within the
   *     schemaMetadata)
   */
  public static Set<Urn> getSchemaFieldAliases(
      @Nonnull Urn datasetUrn,
      @Nonnull SchemaMetadata schemaMetadata,
      @Nonnull SchemaField schemaField) {

    Urn downgradedUrn =
        downgradeSchemaFieldUrn(
            generateSchemaFieldUrn(datasetUrn, downgradeFieldPath(schemaField.getFieldPath())));

    // Find all collisions after v2 -> v1 conversion
    HashSet<Urn> aliases =
        schemaMetadata.getFields().stream()
            .map(
                field ->
                    Pair.of(
                        generateSchemaFieldUrn(
                            datasetUrn, downgradeFieldPath(field.getFieldPath())),
                        generateSchemaFieldUrn(datasetUrn, field.getFieldPath())))
            .filter(pair -> pair.getFirst().equals(downgradedUrn))
            .map(Pair::getSecond)
            .collect(Collectors.toCollection(HashSet::new));

    if (!aliases.isEmpty()) {
      // if v2 -> v1
      aliases.add(downgradedUrn);
    }

    return aliases;
  }

  @VisibleForTesting
  @Nonnull
  static Urn downgradeSchemaFieldUrn(@Nonnull Urn schemaFieldUrn) {
    return generateSchemaFieldUrn(
        UrnUtils.getUrn(schemaFieldUrn.getId()),
        downgradeFieldPath(schemaFieldUrn.getEntityKey().get(1)));
  }

  // https://datahubproject.io/docs/advanced/field-path-spec-v2/#backward-compatibility
  @Nonnull
  private static String downgradeFieldPath(@Nonnull String fieldPath) {
    if (fieldPath.startsWith(FIELD_PATH_V2)) {
      return Arrays.stream(fieldPath.substring(FIELD_PATH_V2.length()).split("[.]"))
          .filter(
              component ->
                  !component.isEmpty() && !(component.startsWith("[") && component.endsWith("]")))
          .collect(Collectors.joining("."));
    }
    return fieldPath;
  }
}
