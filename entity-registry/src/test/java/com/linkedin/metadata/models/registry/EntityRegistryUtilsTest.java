package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.registry.config.Entities;
import com.linkedin.metadata.models.registry.config.Entity;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

public class EntityRegistryUtilsTest {

    @Test
    public void testMergeEntitiesForSingleCase() {
        Entity entity1Part1 = new Entity("entity1", "doc1", "keyAspect1", List.of("entity1aspect1"));
        Entities entities1 = new Entities("dummyid", List.of(entity1Part1), List.of());

        Entity entity1Part2 = new Entity("entity1", "doc1", "keyAspect1", List.of("entity1aspect2"));
        Entities entities2 = new Entities("dummyid", List.of(entity1Part2), List.of());

        Entity entity1Final = new Entity("entity1", "doc1", "keyAspect1", List.of("entity1aspect1", "entity1aspect2"));
        Entities entities1Final = new Entities(
                "dummyid",
                List.of(entity1Final),
                List.of()
        );

        Entities afterMerge1 = EntityRegistryUtils.mergeEntities(List.of(entities1));
        assertEquals(afterMerge1, entities1);

        Entities afterMerge2 = EntityRegistryUtils.mergeEntities(List.of(entities1, entities2));
        assertEquals(afterMerge2, entities1Final);
    }

    @Test
    public void testMergeEntitiesDuplicateException() {
        Entity entity1Part1 = new Entity("entity1", "doc1", "keyAspect1", List.of("entity1aspect1"));
        Entities entities1 = new Entities("dummyid", List.of(entity1Part1), List.of());

        Entity entity1Part2 = new Entity("entity1", "doc1", "keyAspect1", List.of("entity1aspect1", "entity1aspect2"));
        Entities entities2 = new Entities("dummyid", List.of(entity1Part2), List.of());

        assertThrows(() -> EntityRegistryUtils.mergeEntities(List.of(entities1, entities2)));

    }
}
