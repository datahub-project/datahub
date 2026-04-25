package com.datahub.authorization;

import static org.testng.Assert.*;

import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests for {@link EntitySpec} */
public class EntitySpecTest {

  @Test
  public void testConstructorWithProposedAspects() {
    // Given
    String type = "dataset";
    String entity = "urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)";
    Map<String, RecordTemplate> proposedAspects = new HashMap<>();

    // When
    EntitySpec entitySpec = new EntitySpec(type, entity, proposedAspects);

    // Then
    assertEquals(entitySpec.getType(), type);
    assertEquals(entitySpec.getEntity(), entity);
    assertEquals(entitySpec.getProposedAspects(), proposedAspects);
  }

  @Test
  public void testConstructorWithoutProposedAspects() {
    // Given
    String type = "dataset";
    String entity = "urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)";

    // When
    EntitySpec entitySpec = new EntitySpec(type, entity);

    // Then
    assertEquals(entitySpec.getType(), type);
    assertEquals(entitySpec.getEntity(), entity);
    assertNull(entitySpec.getProposedAspects());
  }

  @Test
  public void testConstructorWithNullProposedAspects() {
    // Given
    String type = "chart";
    String entity = "urn:li:chart:(looker,dashboard_element.123)";

    // When
    EntitySpec entitySpec = new EntitySpec(type, entity, null);

    // Then
    assertEquals(entitySpec.getType(), type);
    assertEquals(entitySpec.getEntity(), entity);
    assertNull(entitySpec.getProposedAspects());
  }

  @Test
  public void testEqualsAndHashCode() {
    // Given
    String type = "dataset";
    String entity = "urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)";
    Map<String, RecordTemplate> proposedAspects = new HashMap<>();

    EntitySpec entitySpec1 = new EntitySpec(type, entity, proposedAspects);
    EntitySpec entitySpec2 = new EntitySpec(type, entity, proposedAspects);
    EntitySpec entitySpec3 = new EntitySpec(type, entity, null);

    // Then
    assertEquals(entitySpec1, entitySpec2);
    assertEquals(entitySpec1.hashCode(), entitySpec2.hashCode());
    assertNotEquals(entitySpec1, entitySpec3);
  }

  @Test
  public void testToString() {
    // Given
    String type = "corpGroup";
    String entity = "urn:li:corpGroup:engineering";

    // When
    EntitySpec entitySpec = new EntitySpec(type, entity);

    // Then
    String toString = entitySpec.toString();
    assertNotNull(toString);
    assertTrue(toString.contains(type));
    assertTrue(toString.contains(entity));
  }

  @Test
  public void testDifferentEntityTypes() {
    // Test various entity types
    EntitySpec datasetSpec = new EntitySpec("dataset", "urn:li:dataset:123");
    EntitySpec chartSpec = new EntitySpec("chart", "urn:li:chart:456");
    EntitySpec dashboardSpec = new EntitySpec("dashboard", "urn:li:dashboard:789");
    EntitySpec corpGroupSpec = new EntitySpec("corpGroup", "urn:li:corpGroup:group1");

    assertNotEquals(datasetSpec, chartSpec);
    assertNotEquals(chartSpec, dashboardSpec);
    assertNotEquals(dashboardSpec, corpGroupSpec);
  }
}
