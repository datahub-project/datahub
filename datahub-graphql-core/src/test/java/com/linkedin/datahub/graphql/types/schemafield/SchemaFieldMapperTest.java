package com.linkedin.datahub.graphql.types.schemafield;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class SchemaFieldMapperTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_FIELD_PATH = "field1";
  private static final String TEST_SCHEMA_FIELD_URN =
      "urn:li:schemaField:(" + TEST_DATASET_URN + "," + TEST_FIELD_PATH + ")";
  private static final String TEST_PARENT_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-parent,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:actor";

  @Test
  public void testSchemaFieldMapperBasic() throws Exception {
    // Create basic schema field key
    Urn schemaFieldUrn = Urn.createFromString(TEST_SCHEMA_FIELD_URN);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(schemaFieldUrn);
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));

    SchemaFieldEntity result = SchemaFieldMapper.map(null, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SCHEMA_FIELD_URN);
    assertEquals(result.getType(), EntityType.SCHEMA_FIELD);
    assertEquals(result.getFieldPath(), TEST_FIELD_PATH);
    assertNotNull(result.getParent());
    assertEquals(result.getParent().getUrn(), TEST_DATASET_URN);
  }

  @Test
  public void testSchemaFieldMapperWithLogicalParent() throws Exception {
    Urn schemaFieldUrn = Urn.createFromString(TEST_SCHEMA_FIELD_URN);
    Urn parentUrn = Urn.createFromString(TEST_PARENT_URN);
    Urn actorUrn = Urn.createFromString(TEST_ACTOR_URN);

    // Create logical parent aspect
    LogicalParent logicalParent = new LogicalParent();
    Edge edge = new Edge();
    edge.setDestinationUrn(parentUrn);
    edge.setCreated(new AuditStamp().setTime(10L).setActor(actorUrn));
    edge.setLastModified(new AuditStamp().setTime(20L).setActor(actorUrn));
    logicalParent.setParent(edge);

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.LOGICAL_PARENT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(logicalParent.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(schemaFieldUrn);
    entityResponse.setAspects(new EnvelopedAspectMap(aspects));

    SchemaFieldEntity result = SchemaFieldMapper.map(null, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SCHEMA_FIELD_URN);
    assertEquals(result.getType(), EntityType.SCHEMA_FIELD);
    assertEquals(result.getFieldPath(), TEST_FIELD_PATH);

    // Verify the schema field parent (from URN parsing)
    assertNotNull(result.getParent());
    assertEquals(result.getParent().getUrn(), TEST_DATASET_URN);

    // Verify the logical parent aspect
    assertNotNull(result.getLogicalParent());
    assertEquals(result.getLogicalParent().getUrn(), TEST_PARENT_URN);
  }

  @Test
  public void testSchemaFieldMapperWithNullLogicalParent() throws Exception {
    Urn schemaFieldUrn = Urn.createFromString(TEST_SCHEMA_FIELD_URN);

    // Create logical parent aspect with null parent (default)
    LogicalParent logicalParent = new LogicalParent();
    // Don't set parent - leave it as default (null)

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.LOGICAL_PARENT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(logicalParent.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(schemaFieldUrn);
    entityResponse.setAspects(new EnvelopedAspectMap(aspects));

    SchemaFieldEntity result = SchemaFieldMapper.map(null, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SCHEMA_FIELD_URN);
    assertEquals(result.getType(), EntityType.SCHEMA_FIELD);
    assertEquals(result.getFieldPath(), TEST_FIELD_PATH);

    // Verify the schema field parent (from URN parsing) is still set
    assertNotNull(result.getParent());
    assertEquals(result.getParent().getUrn(), TEST_DATASET_URN);

    // Verify the logical parent aspect is null
    assertNull(result.getLogicalParent());
  }

  @Test
  public void testSchemaFieldMapperWithoutLogicalParent() throws Exception {
    Urn schemaFieldUrn = Urn.createFromString(TEST_SCHEMA_FIELD_URN);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(schemaFieldUrn);
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));

    SchemaFieldEntity result = SchemaFieldMapper.map(null, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SCHEMA_FIELD_URN);
    assertEquals(result.getType(), EntityType.SCHEMA_FIELD);
    assertEquals(result.getFieldPath(), TEST_FIELD_PATH);

    // Verify the schema field parent (from URN parsing) is still set
    assertNotNull(result.getParent());
    assertEquals(result.getParent().getUrn(), TEST_DATASET_URN);

    // Verify no logical parent aspect
    assertNull(result.getLogicalParent());
  }
}
