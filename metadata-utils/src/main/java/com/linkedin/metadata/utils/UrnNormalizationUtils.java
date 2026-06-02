package com.linkedin.metadata.utils;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for normalizing URNs to enable case-insensitive lineage matching.
 *
 * <p>This class provides methods to normalize URNs by converting platform names and dataset names
 * to lowercase while preserving FabricType (environment) as uppercase to maintain valid URN format.
 *
 * <p>Example:
 *
 * <pre>
 * Input:  urn:li:dataset:(urn:li:dataPlatform:Snowflake,DB.SCHEMA.TABLE,PROD)
 * Output: urn:li:dataset:(urn:li:dataplatform:snowflake,db.schema.table,PROD)
 * </pre>
 *
 * <p>For schema field URNs, both the parent dataset URN and field path are normalized:
 *
 * <pre>
 * Input:  urn:li:schemaField:(urn:li:dataset:(...,DB.TABLE,PROD),FieldName)
 * Output: urn:li:schemaField:(urn:li:dataset:(...,db.table,PROD),fieldname)
 * </pre>
 */
@Slf4j
public class UrnNormalizationUtils {

  private static final String DATASET_ENTITY_TYPE = "dataset";
  private static final String SCHEMA_FIELD_ENTITY_TYPE = "schemaField";

  private UrnNormalizationUtils() {}

  /**
   * Normalize a URN to lowercase while preserving FabricType (env) as uppercase.
   *
   * <p>For dataset URNs: - Platform URN is lowercased (e.g., urn:li:dataPlatform:Snowflake →
   * urn:li:dataplatform:snowflake) - Dataset name is lowercased (e.g., DB.Table → db.table) -
   * FabricType remains uppercase (e.g., PROD stays PROD)
   *
   * <p>For schema field URNs: - Parent dataset URN is normalized recursively - Field path is
   * lowercased
   *
   * <p>For other URN types: - Entire URN is lowercased
   *
   * @param urn the URN to normalize
   * @return normalized URN with lowercase platform/dataset names and uppercase FabricType
   */
  @Nonnull
  public static Urn normalizeUrn(@Nonnull Urn urn) {
    try {
      if (DATASET_ENTITY_TYPE.equals(urn.getEntityType())) {
        return normalizeDatasetUrn(urn);
      } else if (SCHEMA_FIELD_ENTITY_TYPE.equals(urn.getEntityType())) {
        return normalizeSchemaFieldUrn(urn);
      } else {
        // For other entity types, simple lowercase normalization
        return UrnUtils.getUrn(urn.toString().toLowerCase());
      }
    } catch (Exception e) {
      log.warn("Failed to normalize URN: {}. Returning original URN.", urn, e);
      return urn;
    }
  }

  /**
   * Normalize a dataset URN by lowercasing platform and dataset name while preserving FabricType.
   *
   * @param urn the dataset URN to normalize
   * @return normalized dataset URN
   */
  @Nonnull
  private static Urn normalizeDatasetUrn(@Nonnull Urn urn) {
    try {
      DatasetUrn datasetUrn = DatasetUrn.createFromUrn(urn);

      // Normalize platform URN (lowercase)
      DataPlatformUrn platformUrn = datasetUrn.getPlatformEntity();
      String normalizedPlatformName = platformUrn.getPlatformNameEntity().toLowerCase();
      DataPlatformUrn normalizedPlatformUrn = new DataPlatformUrn(normalizedPlatformName);

      // Normalize dataset name (lowercase)
      String normalizedDatasetName = datasetUrn.getDatasetNameEntity().toLowerCase();

      // Keep FabricType unchanged (already uppercase enum values)
      FabricType fabricType = datasetUrn.getOriginEntity();

      return new DatasetUrn(normalizedPlatformUrn, normalizedDatasetName, fabricType);
    } catch (URISyntaxException e) {
      log.warn("Failed to parse dataset URN: {}. Returning original URN.", urn, e);
      return urn;
    }
  }

  /**
   * Normalize a schema field URN by normalizing the parent dataset URN and lowercasing the field
   * path.
   *
   * <p>Schema field URN format: urn:li:schemaField:(parentDatasetUrn,fieldPath)
   *
   * @param urn the schema field URN to normalize
   * @return normalized schema field URN
   */
  @Nonnull
  private static Urn normalizeSchemaFieldUrn(@Nonnull Urn urn) {
    try {
      // Schema field URN has format: urn:li:schemaField:(parentDatasetUrn,fieldPath)
      String parentDatasetUrnString = urn.getEntityKey().get(0);
      String fieldPath = urn.getEntityKey().get(1);

      // Normalize the parent dataset URN
      Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnString);
      Urn normalizedParentUrn = normalizeUrn(parentDatasetUrn);

      // Normalize field path (lowercase)
      String normalizedFieldPath = fieldPath.toLowerCase();

      // Reconstruct the schema field URN
      return Urn.createFromTuple(
          SCHEMA_FIELD_ENTITY_TYPE, normalizedParentUrn.toString(), normalizedFieldPath);
    } catch (Exception e) {
      log.warn("Failed to normalize schema field URN: {}. Returning original URN.", urn, e);
      return urn;
    }
  }

  /**
   * Check if a URN needs normalization (contains uppercase characters in platform or dataset name).
   *
   * <p>Returns true if: - For dataset URNs: platform name or dataset name contains uppercase - For
   * schema field URNs: parent dataset URN or field path contains uppercase - For other URNs: URN
   * string contains uppercase
   *
   * @param urn the URN to check
   * @return true if URN needs normalization, false if already normalized
   */
  public static boolean needsNormalization(@Nonnull Urn urn) {
    try {
      if (DATASET_ENTITY_TYPE.equals(urn.getEntityType())) {
        DatasetUrn datasetUrn = DatasetUrn.createFromUrn(urn);

        // Check if platform name has uppercase
        String platformName = datasetUrn.getPlatformEntity().getPlatformNameEntity();
        if (!platformName.equals(platformName.toLowerCase())) {
          return true;
        }

        // Check if dataset name has uppercase
        String datasetName = datasetUrn.getDatasetNameEntity();
        return !datasetName.equals(datasetName.toLowerCase());
      } else if (SCHEMA_FIELD_ENTITY_TYPE.equals(urn.getEntityType())) {
        // Check parent dataset URN
        String parentDatasetUrnString = urn.getEntityKey().get(0);
        Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnString);
        if (needsNormalization(parentDatasetUrn)) {
          return true;
        }

        // Check field path
        String fieldPath = urn.getEntityKey().get(1);
        return !fieldPath.equals(fieldPath.toLowerCase());
      } else {
        // For other entity types, check if entire URN has uppercase
        String urnString = urn.toString();
        return !urnString.equals(urnString.toLowerCase());
      }
    } catch (Exception e) {
      log.warn(
          "Failed to check if URN needs normalization: {}. Assuming no normalization needed.",
          urn,
          e);
      return false;
    }
  }
}
