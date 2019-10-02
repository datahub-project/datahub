package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.testing.RelationshipBar;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.EntityBar;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class Neo4jQueryDAOTest {

  private Neo4jTestServerBuilder _serverBuilder;
  private Neo4jQueryDAO _dao;
  private Neo4jGraphWriterDAO _writer;

  @BeforeMethod
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();

    final Driver driver = GraphDatabase.driver(_serverBuilder.boltURI());
    _dao = new Neo4jQueryDAO(driver);
    _writer = new Neo4jGraphWriterDAO(driver);
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Test
  public void testFindEntityByUrn() throws Exception {
    Urn urn = makeUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn).setValue("foo");

    _writer.addEntity(entity);

    Filter filter = makeFilter("urn", urn.toString());
    List<EntityFoo> found = _dao.findEntities(EntityFoo.class, filter, -1, -1);

    assertEquals(found.size(), 1);
    assertEquals(found.get(0), entity);
  }

  @Test
  public void testFindEntityByAttribute() throws Exception {
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo");
    _writer.addEntity(entity2);

    // find by removed
    Filter filter1 = makeFilter("value", "foo");
    List<EntityFoo> found = _dao.findEntities(EntityFoo.class, filter1, -1, -1);
    assertEquals(found, Arrays.asList(entity1, entity2));

    // find with limit
    found = _dao.findEntities(EntityFoo.class, filter1, -1, 1);
    assertEquals(found, Collections.singletonList(entity1));

    // find with offset
    found = _dao.findEntities(EntityFoo.class, filter1, 1, -1);
    assertEquals(found, Collections.singletonList(entity2));
  }

  @Test
  public void testFindEntityByQuery() throws Exception {
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    Statement statement = new Statement("MATCH (n {value:\"foo\"}) RETURN n ORDER BY n.urn", Collections.emptyMap());
    List<EntityFoo> found = _dao.findEntities(EntityFoo.class, statement);
    assertEquals(found.size(), 1);
    assertEquals(found.get(0), entity1);

    Urn urn2 = makeUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo");
    _writer.addEntity(entity2);

    found = _dao.findEntities(EntityFoo.class, statement);
    assertEquals(found.size(), 2);
    assertEquals(found.get(0), entity1);
    assertEquals(found.get(1), entity2);
  }

  @Test
  public void testFindEntityWithOneRelationship() throws Exception {
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _writer.addEntity(entity2);

    // create relationship urn1 -> urn2
    RelationshipFoo relationshipFoo1To2 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipFoo1To2);

    // test source filter with urn
    Filter sourceFilter = makeFilter("urn", urn1.toString());
    List<EntityBar> result =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityBar.class, emptyFilter(), RelationshipFoo.class,
            emptyFilter(), 0, 10);

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), entity2);

    // test source filter with value
    sourceFilter = makeFilter("value", "foo");
    result = _dao.findEntities(EntityFoo.class, sourceFilter, EntityBar.class, emptyFilter(), RelationshipFoo.class,
        emptyFilter(), 0, 10);

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), entity2);

    // test source filter with wrong value
    Filter sourceFilter2 = makeFilter("value", "wrongValue");
    result = _dao.findEntities(EntityFoo.class, sourceFilter2, EntityBar.class, emptyFilter(), RelationshipFoo.class,
        emptyFilter(), 0, 10);

    assertEquals(result.size(), 0);

    // add third entity
    Urn urn3 = makeUrn(3);
    EntityBar entity3 = new EntityBar().setUrn(urn3).setValue("bar3");
    _writer.addEntity(entity3);

    // create another relationship urn1 -> urn3
    RelationshipFoo relationshipFoo1To3 = new RelationshipFoo().setSource(urn1).setDestination(urn3);
    _writer.addRelationship(relationshipFoo1To3);

    // test source filter with urn and get two destination entities back
    List<EntityBar> result2 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityBar.class, emptyFilter(), RelationshipFoo.class,
            emptyFilter(), 0, 10);
    assertEquals(result2.size(), 2);
    assertEquals(result2.get(0), entity2);
    assertEquals(result2.get(1), entity3);

    // add fourth entity
    Urn urn4 = makeUrn(4);
    EntityBar entity4 = new EntityBar().setUrn(urn4).setValue("bar4");
    _writer.addEntity(entity4);

    // add a different type of relationship urn1 -> urn4
    RelationshipBar relationshipBar1To4 = new RelationshipBar().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipBar1To4);

    // should still only return two entities as before, entity4 should not be returned
    List<EntityBar> result3 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityBar.class, emptyFilter(), RelationshipFoo.class,
            emptyFilter(), 0, 10);
    assertEquals(result3.size(), 2);
    assertEquals(result3.get(0), entity2);
    assertEquals(result3.get(1), entity3);
  }

  @Test
  public void testFindRelationship() throws Exception {
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _writer.addEntity(entity2);

    // create relationship urn1 -> urn2
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationship);

    // find by source
    Filter filter1 = makeFilter("urn", urn1.toString());
    List<RelationshipFoo> found =
        _dao.findRelationshipsFromSource(null, filter1, RelationshipFoo.class, emptyFilter(), -1, -1);
    assertEquals(found, Collections.singletonList(relationship));

    // find by destination
    Filter filter2 = makeFilter("urn", urn2.toString());
    found =
        _dao.findRelationshipsFromDestination(EntityBar.class, filter2, RelationshipFoo.class, emptyFilter(), -1, -1);
    assertEquals(found, Collections.singletonList(relationship));

    // find by source and destination
    found = _dao.findRelationships(null, filter1, null, filter2, RelationshipFoo.class, emptyFilter(), 0, 10);
    assertEquals(found, Collections.singletonList(relationship));
  }

  @Test
  public void testFindRelationshipByQuery() throws Exception {
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _writer.addEntity(entity2);

    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationship);

    // with type
    Map<String, Object> params = new HashMap<>();
    params.put("src", urn1.toString());
    Statement statement1 = new Statement("MATCH (src {urn:$src})-[r]->(dest) RETURN src, r, dest", params);
    List<RelationshipFoo> found = _dao.findRelationships(RelationshipFoo.class, statement1);
    assertEquals(found, Collections.singletonList(relationship));

    // without type
    params.put("dest", urn2.toString());
    Statement statement2 = new Statement("MATCH (src {urn:$src})-[r]->(dest {urn:$dest}) RETURN src, r, dest", params);
    List<RecordTemplate> foundNoType = _dao.findMixedTypesRelationships(statement2);
    assertEquals(foundNoType, Collections.singletonList(relationship));
  }

  private Filter emptyFilter() {
    return new Filter().setCriteria(new CriterionArray());
  }

  private Filter makeFilter(String field, String value) {
    Filter filter = new Filter().setCriteria(new CriterionArray());
    filter.getCriteria().add(new Criterion().setField(field).setValue(value).setCondition(Condition.EQUAL));
    return filter;
  }
}
