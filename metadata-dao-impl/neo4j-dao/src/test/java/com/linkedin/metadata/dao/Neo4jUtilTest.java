package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.testing.EntityBar;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class Neo4jUtilTest {

  @Test
  public void testEntityToNode() {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn).setValue("foo");

    Map<String, Object> actual = entityToNode(entity);

    Map<String, Object> expected = new HashMap<>();
    expected.put("urn", urn.toString());
    expected.put("value", "foo");

    assertEquals(actual, expected);
  }

  @Test
  public void testRelationshipToEdge() {
    Urn urn = makeUrn(1);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn).setDestination(urn);

    Map<String, Object> fields = relationshipToEdge(relationship);

    assertTrue(fields.isEmpty());
  }

  @Test
  public void testRelationshipToCriteria() {
    Urn urn = makeUrn(1);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn).setDestination(urn);

    String criteria = relationshipToCriteria(relationship);

    assertTrue(criteria.isEmpty());

    relationship.data().put("bar", urn.toString());
    relationship.data().put("foo", 2);

    criteria = relationshipToCriteria(relationship);

    assertEquals(criteria, "{bar:\"urn:li:testing:1\",foo:2}");
  }

  @Test
  public void testFilterToCriteria() {
    Filter filter = newFilter("a", "b");

    assertEquals(filterToCriteria(filter), "{a:\"b\"}");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testFilterToCriteriaNonEqual() {
    Filter filter = new Filter().setCriteria(new CriterionArray());
    filter.getCriteria().add(new Criterion().setField("a").setValue("b").setCondition(Condition.CONTAIN));

    filterToCriteria(filter);
    fail("no exception");
  }

  @Test
  public void testNodeToEntity() {
    FooUrn urn = makeFooUrn(1);
    Node node = new InternalNode(2, Collections.singletonList(EntityFoo.class.getCanonicalName()),
        Collections.singletonMap("urn", new StringValue(urn.toString())));

    // with type class
    EntityFoo entity1 = nodeToEntity(EntityFoo.class, node);

    assertEquals(entity1, new EntityFoo().setUrn(urn));

    // without type class
    RecordTemplate entity2 = nodeToEntity(node);

    assertEquals(entity2.getClass(), EntityFoo.class);
    assertEquals(entity2, new EntityFoo().setUrn(urn));
  }

  @Test
  public void testEdgeToRelationship() {
    Urn urn1 = makeUrn(1);
    Node node1 = new InternalNode(3, Collections.singletonList(EntityFoo.class.getCanonicalName()),
        Collections.singletonMap("urn", new StringValue(urn1.toString())));

    Urn urn2 = makeUrn(2);
    Node node2 = new InternalNode(4, Collections.singletonList(EntityBar.class.getCanonicalName()),
        Collections.singletonMap("urn", new StringValue(urn2.toString())));

    Relationship edge = new InternalRelationship(5, 3, 4, RelationshipFoo.class.getCanonicalName());

    // with type class
    RelationshipFoo relationship1 = edgeToRelationship(RelationshipFoo.class, node1, node2, edge);

    RelationshipFoo expected = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    assertEquals(relationship1, expected);

    // without type class
    RecordTemplate relationship2 = edgeToRelationship(node1, node2, edge);

    assertEquals(relationship2.getClass(), RelationshipFoo.class);
    assertEquals(relationship2, expected);
  }

  @Test
  public void testEdgeToRelationshipWithoutType() {
    FooUrn fooUrn = makeFooUrn(1);
    Node fooNode = new InternalNode(6, Collections.singletonList("foo"),
        Collections.singletonMap("urn", new StringValue(fooUrn.toString())));

    BarUrn barUrn = makeBarUrn(1);
    Node barNode = new InternalNode(7, Collections.singletonList("bar"),
        Collections.singletonMap("urn", new StringValue(barUrn.toString())));

    Relationship edge = new InternalRelationship(8, 6, 7, RelationshipFoo.class.getCanonicalName());

    RelationshipFoo relationship = edgeToRelationship(RelationshipFoo.class, fooNode, barNode, edge);

    assertEquals(relationship, new RelationshipFoo().setSource(fooUrn).setDestination(barUrn));
  }

  @Test
  public void testGetUrn() {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn).setDestination(urn);

    assertEquals(getUrnFromEntity(entity), urn);
    assertEquals(getSourceUrnFromRelationship(relationship), urn);
    assertEquals(getDestinationUrnFromRelationship(relationship), urn);
  }

  @Test
  public void testGetType() {
    assertEquals(getType(new EntityFoo()), "`com.linkedin.testing.EntityFoo`");
    assertEquals(getType(EntityBar.class), "`com.linkedin.testing.EntityBar`");
  }

  @Test
  public void testGetTypeOrEmptyString() {
    assertEquals(getTypeOrEmptyString(EntityBar.class), ":`com.linkedin.testing.EntityBar`");
    assertEquals(getTypeOrEmptyString(null), "");
  }

  @Test
  public void testCreateRelationshipFilter() {
    String field = "field";
    String value = "value";
    RelationshipDirection direction = RelationshipDirection.OUTGOING;

    RelationshipFilter relationshipFilter = new RelationshipFilter().setCriteria(new CriterionArray(
        Collections.singletonList(new Criterion().setField(field).setValue(value).setCondition(Condition.EQUAL))))
        .setDirection(direction);

    assertEquals(createRelationshipFilter(field, value, direction), relationshipFilter);
  }

  @Test
  public void testPathSegmentToRecordList() {
    FooUrn fooUrn = makeFooUrn(1);
    Node fooNode = new InternalNode(0, Collections.singletonList("com.linkedin.testing.EntityFoo"),
        Collections.singletonMap("urn", new StringValue(fooUrn.toString())));

    BarUrn barUrn = makeBarUrn(1);
    Node barNode = new InternalNode(1, Collections.singletonList("com.linkedin.testing.EntityBar"),
        Collections.singletonMap("urn", new StringValue(barUrn.toString())));

    Relationship edge = new InternalRelationship(2, 0, 1, RelationshipFoo.class.getCanonicalName(),
        new HashMap<String, Value>() {
          {
            put("intField", new IntegerValue(42));
            put("type", new StringValue("dummyType"));
          }
        });

    Path path = new InternalPath(fooNode, edge, barNode);

    List<RecordTemplate> pathList = pathSegmentToRecordList(path.iterator().next());

    assertTrue(pathList.get(0) instanceof EntityFoo);
    assertTrue(pathList.get(1) instanceof RelationshipFoo);
    assertTrue(pathList.get(2) instanceof EntityBar);

    assertEquals(pathList.get(0), new EntityFoo().setUrn(fooUrn));
    assertTrue(DataTemplateUtil.areEqual(pathList.get(1),
        new RelationshipFoo()
            .setIntField(42)
            .setSource(fooUrn)
            .setDestination(barUrn)
            .setType("dummyType")));
    assertEquals(pathList.get(2), new EntityBar().setUrn(barUrn));
  }
}
