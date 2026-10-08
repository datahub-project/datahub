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
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

/** Container shapes the shipped aspects don't all exercise: maps, unions and nested records. */
public class UnknownEntityUrnStripperTest {

  private static final String KNOWN = "urn:li:dataset:d";
  private static final String UNKNOWN = "urn:li:entityFromNewerBuild:x";

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
  public void testRequiredUrnUpToTheRootIsLeftForValidation() {
    DataMap data = new DataMap(Map.of("owner", UNKNOWN));

    assertEquals(UnknownEntityUrnStripper.strip(record(data), registry()), 0);
    assertTrue(data.containsKey("owner"));
  }

  private static RecordTemplate record(DataMap data) {
    return new RecordTemplate(data, SCHEMA) {};
  }

  private static EntityRegistry registry() {
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    when(registry.findEntitySpec("dataset")).thenReturn(Optional.of(mock(EntitySpec.class)));
    return registry;
  }
}
