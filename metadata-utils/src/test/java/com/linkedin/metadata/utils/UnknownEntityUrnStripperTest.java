package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Container shapes the shipped aspects don't all exercise: maps, unions and nested records. */
public class UnknownEntityUrnStripperTest {

  private static final String KNOWN = "urn:li:dataset:d";
  private static final String UNKNOWN = "urn:li:entityFromNewerBuild:x";
  private static final String PLATFORM = "urn:li:dataPlatform:kafka";
  private static final String DATASET = "urn:li:dataset:(" + PLATFORM + ",orders,PROD)";

  private static final RecordDataSchema SCHEMA =
      (RecordDataSchema)
          DataTemplateUtil.parseSchema(
              "{ \"type\": \"record\", \"name\": \"Shapes\", \"namespace\": \"test\", \"fields\": ["
                  + " { \"name\": \"owner\", \"type\": "
                  + "   { \"type\": \"typeref\", \"name\": \"OwnerUrn\", \"ref\": \"string\" } },"
                  + " { \"name\": \"byName\", \"type\": { \"type\": \"map\", \"values\": \"OwnerUrn\" },"
                  + "   \"optional\": true },"
                  + " { \"name\": \"choices\", \"optional\": true, \"type\": { \"type\": \"array\","
                  + "   \"items\": [ \"OwnerUrn\", \"int\" ] } },"
                  + " { \"name\": \"nested\", \"optional\": true, \"type\": { \"type\": \"record\","
                  + "   \"name\": \"Nested\", \"fields\": [ { \"name\": \"target\", \"type\": \"OwnerUrn\" },"
                  + "   { \"name\": \"note\", \"type\": \"string\" } ] } },"
                  + " { \"name\": \"label\", \"type\": \"string\", \"optional\": true } ] }");

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testMapEntriesUnionMembersAndOptionalRecordsAreRemoved() {
    DataMap data =
        new DataMap(
            Map.of(
                "owner", KNOWN,
                "byName", new DataMap(Map.of("a", KNOWN, "b", UNKNOWN)),
                "choices",
                    new DataList(
                        List.of(
                            new DataMap(Map.of("string", UNKNOWN)), new DataMap(Map.of("int", 1)))),
                "nested", new DataMap(Map.of("target", UNKNOWN, "note", "n")),
                "label", UNKNOWN));

    int removed = UnknownEntityUrnStripper.strip(record(data), registry());

    assertEquals(removed, 3);
    assertEquals(data.getDataMap("byName"), new DataMap(Map.of("a", KNOWN)));
    // A union member can't be removed on its own, so the array element holding it goes.
    assertEquals(data.getDataList("choices").size(), 1);
    // A required urn inside an optional record removes that record.
    assertFalse(data.containsKey("nested"));
    // Plain strings that look like urns are not references.
    assertEquals(data.getString("label"), UNKNOWN);
  }

  @Test
  public void testUrnsNestingAnUnknownEntityTypeInTheirKeyAreRemoved() {
    // Nested urns are the urn-typed key fields, at any depth; urn-shaped text in a string key
    // field (e.g. a dataset name) is not a reference.
    DataMap data =
        new DataMap(
            Map.of(
                "owner",
                KNOWN,
                "byName",
                new DataMap(
                    Map.of(
                        "known", "urn:li:schemaField:(urn:li:schemaField:(" + DATASET + ",a),col)",
                        "deep", "urn:li:schemaField:(urn:li:schemaField:(" + UNKNOWN + ",a),col)",
                        "urnShapedName", "urn:li:dataset:(" + PLATFORM + "," + UNKNOWN + ",PROD)",
                        "text", "urn:li:dataset:(" + PLATFORM + ",see " + UNKNOWN + ",PROD)"))));

    assertEquals(UnknownEntityUrnStripper.strip(record(data), registry()), 1);
    assertEquals(
        data.getDataMap("byName").keySet(), java.util.Set.of("known", "urnShapedName", "text"));
  }

  @Test
  public void testRequiredUrnUpToTheRootIsLeftForValidation() {
    DataMap data = new DataMap(Map.of("owner", UNKNOWN));

    assertEquals(UnknownEntityUrnStripper.strip(record(data), registry()), 0);
    assertTrue(data.containsKey("owner"));
  }

  @Test
  public void testReferencesUnknownEntityTypeDetectsWithoutChangingTheAspect() {
    DataMap unknownRoot = new DataMap(Map.of("owner", UNKNOWN, "label", "x"));
    DataMap unknownNested =
        new DataMap(Map.of("owner", KNOWN, "byName", new DataMap(Map.of("a", UNKNOWN))));
    DataMap known = new DataMap(Map.of("owner", KNOWN, "label", UNKNOWN));

    assertTrue(
        UnknownEntityUrnStripper.referencesUnknownEntityType(record(unknownRoot), registry()));
    assertTrue(
        UnknownEntityUrnStripper.referencesUnknownEntityType(record(unknownNested), registry()));
    assertFalse(UnknownEntityUrnStripper.referencesUnknownEntityType(record(known), registry()));
    assertEquals(unknownNested.getDataMap("byName").size(), 1);
  }

  @Test
  public void testUrnNestingAnUnknownTypeInItsKeyIsRemoved() {
    // A known outer type whose urn-typed key field references an entity type only a newer version
    // has, e.g. a schema field or monitor of a new entity type.
    String nestedUnknown = "urn:li:schemaField:(" + UNKNOWN + ",col)";
    String nestedKnown = "urn:li:schemaField:(" + DATASET + ",col)";
    DataMap data =
        new DataMap(
            Map.of(
                "owner",
                KNOWN,
                "byName",
                new DataMap(Map.of("a", nestedKnown, "b", nestedUnknown))));

    assertEquals(UnknownEntityUrnStripper.strip(record(data), registry()), 1);
    assertEquals(data.getDataMap("byName"), new DataMap(Map.of("a", nestedKnown)));
  }

  @Test
  public void testNestedUrnsAreTheUrnTypedKeyFields() {
    // Nested urns are found by the key's schema, not by searching the text.
    DataMap data =
        new DataMap(
            Map.of(
                "owner",
                KNOWN,
                "byName",
                new DataMap(
                    Map.of(
                        // Text inside a string key field is not a nested urn, whatever it holds.
                        "fieldText", "urn:li:schemaField:(" + DATASET + ",f(" + UNKNOWN + "))",
                        // The urn-typed field is, at any depth.
                        "parent", "urn:li:schemaField:(" + UNKNOWN + ",col)",
                        // A malformed nested urn is left for validation to reject.
                        "malformedParent", "urn:li:schemaField:(urn:li::x,col)"))));

    assertEquals(UnknownEntityUrnStripper.strip(record(data), registry()), 1);
    assertEquals(
        data.getDataMap("byName").keySet(), java.util.Set.of("fieldText", "malformedParent"));
  }

  private static RecordTemplate record(DataMap data) {
    return new RecordTemplate(data, SCHEMA) {};
  }

  private static EntityRegistry registry() {
    Optional<EntitySpec> dataset = entity(new DatasetKey().schema());
    Optional<EntitySpec> schemaField = entity(new SchemaFieldKey().schema());
    Optional<EntitySpec> dataPlatform = entity(new DataPlatformKey().schema());
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    when(registry.findEntitySpec("dataset")).thenReturn(dataset);
    when(registry.findEntitySpec("schemaField")).thenReturn(schemaField);
    when(registry.findEntitySpec("dataPlatform")).thenReturn(dataPlatform);
    return registry;
  }

  private static Optional<EntitySpec> entity(RecordDataSchema keySchema) {
    AspectSpec key = mock(AspectSpec.class);
    when(key.getPegasusSchema()).thenReturn(keySchema);
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getKeyAspectSpec()).thenReturn(key);
    return Optional.of(entitySpec);
  }
}
