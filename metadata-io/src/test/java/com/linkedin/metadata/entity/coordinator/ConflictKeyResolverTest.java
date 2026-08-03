package com.linkedin.metadata.entity.coordinator;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.SortedSet;
import org.testng.annotations.Test;

public class ConflictKeyResolverTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_schema.events,PROD)";
  private static final String SCHEMA_FIELD_URN = "urn:li:schemaField:(" + DATASET_URN + ",col_a)";

  private final ConflictKeyResolver resolver = new ConflictKeyResolver();

  @Test
  public void schemaFieldResolvesToParentDatasetLinkageKey() {
    ConflictKey key = resolver.resolve(AspectKey.latest(SCHEMA_FIELD_URN, "documentation"));

    assertEquals(key, ConflictKey.of("SCHEMA_FIELD_LINKAGE", DATASET_URN));
  }

  @Test
  public void plainEntityResolvesToItsOwnUrn() {
    ConflictKey key = resolver.resolve(AspectKey.latest(DATASET_URN, "status"));

    assertEquals(key, ConflictKey.of("dataset", DATASET_URN));
  }

  @Test
  public void malformedSchemaFieldFallsBackToGenericRule() {
    // A schemaField URN whose key cannot be split into (parent, fieldPath) — parseSchemaFieldUrn
    // returns empty, so we coordinate on the schemaField URN itself rather than throwing.
    String malformed = "urn:li:schemaField:(only_one_part)";

    ConflictKey key = resolver.resolve(AspectKey.latest(malformed, "documentation"));

    assertEquals(key, ConflictKey.of("schemaField", malformed));
  }

  @Test
  public void resolveAllDeduplicatesSchemaFieldsOntoOneDatasetKey() {
    String siblingField = "urn:li:schemaField:(" + DATASET_URN + ",col_b)";

    SortedSet<ConflictKey> keys =
        resolver.resolveAll(
            List.of(
                AspectKey.latest(SCHEMA_FIELD_URN, "documentation"),
                AspectKey.latest(siblingField, "documentation"),
                AspectKey.latest(DATASET_URN, "status")));

    // Both schemaFields collapse onto the same parent-dataset linkage key; the dataset's own key is
    // a distinct domain — two conflict keys total.
    assertEquals(keys.size(), 2);
  }
}
