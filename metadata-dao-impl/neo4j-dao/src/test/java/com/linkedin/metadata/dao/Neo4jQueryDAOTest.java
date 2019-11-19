package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.testing.EntityBaz;
import com.linkedin.testing.RelationshipBar;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.EntityBar;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Triplet;
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
    // Test interface 1 & 3
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo1");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo2");
    _writer.addEntity(entity2);

    Urn urn3 = makeUrn(3);
    EntityFoo entity3 = new EntityFoo().setUrn(urn3).setValue("foo3");
    _writer.addEntity(entity3);

    Urn urn4 = makeUrn(4);
    EntityBar entity4 = new EntityBar().setUrn(urn4).setValue("bar4");
    _writer.addEntity(entity4);

    Urn urn5 = makeUrn(5);
    EntityBar entity5 = new EntityBar().setUrn(urn5).setValue("bar5");
    _writer.addEntity(entity5);

    // create relationship urn1 -(r:Foo)-> urn2 -(r:Foo)-> urn3 (example use case: ReportTo list)
    // also relationship urn1 -(r:Foo)-> urn4, and urn1 -(r:Bar)-> urn5
    RelationshipFoo relationshipFoo1To2 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipFoo1To2);

    RelationshipFoo relationshipFoo2o3 = new RelationshipFoo().setSource(urn2).setDestination(urn3).setType("apa");
    _writer.addRelationship(relationshipFoo2o3);

    RelationshipFoo relationshipFoo1o4 = new RelationshipFoo().setSource(urn1).setDestination(urn4);
    _writer.addRelationship(relationshipFoo1o4);

    RelationshipBar relationshipBar1o5 = new RelationshipBar().setSource(urn1).setDestination(urn5);
    _writer.addRelationship(relationshipBar1o5);

    // test source filter with urn
    Filter sourceFilter = makeFilter("urn", urn2.toString());

    // use case: the direct reportee to me (urn2)
    List<RecordTemplate> resultIncoming =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.INCOMING), 0, 10);
    assertEquals(resultIncoming.size(), 1);
    assertEquals(resultIncoming.get(0), entity1);

    // use case: the manager I (urn2) am report to
    List<RecordTemplate> resultOutgoing =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultOutgoing.size(), 1);
    assertEquals(resultOutgoing.get(0), entity3);

    // use case: give me my friends at one degree/hop
    List<RecordTemplate> resultUndirected =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.UNDIRECTED), 0, 10);
    assertEquals(resultUndirected.size(), 2);
    assertEquals(resultUndirected.get(0), entity1);
    assertEquals(resultUndirected.get(1), entity3);

    // Test: if dest entity type is not specified, from urn1, it will return a mixed collection of entities like urn2 and urn4
    // urn5 should not be returned because it is based on RelationshipBar.
    sourceFilter = makeFilter("urn", urn1.toString());
    List<RecordTemplate> resultNullDest =
        _dao.findEntities(EntityFoo.class, sourceFilter, null, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultNullDest.size(), 2);
    assertEquals(resultNullDest.get(0), entity2);
    assertEquals(resultNullDest.get(1), entity4);

    // Test: source filter on other attributes
    sourceFilter = makeFilter("value", "foo2");
    List<RecordTemplate> result =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.INCOMING), 0, 10);
    assertEquals(result, resultIncoming);

    //Test relationship filters on the attributes
    Filter filter = makeFilter("type", "apa");
    List<RecordTemplate> resultWithRelationshipFilter =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(filter, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultWithRelationshipFilter.size(), 1);
    assertEquals(resultWithRelationshipFilter.get(0), entity3);

    //Test a wrong value for relationship filters
    Filter relationshipFilterWrongValue = makeFilter("type", "wrongValue");
    List<RecordTemplate> resultWithRelationshipFilter2 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(relationshipFilterWrongValue, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultWithRelationshipFilter2.size(), 0);
  }

  @Test
  public void testFindEntitiesMultiHops() throws Exception {
    // Test interface 5
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo1");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo2");
    _writer.addEntity(entity2);

    Urn urn3 = makeUrn(3);
    EntityFoo entity3 = new EntityFoo().setUrn(urn3).setValue("foo3");
    _writer.addEntity(entity3);

    // create relationship urn1 -> urn2 -> urn3 (use case: ReportTo list)
    RelationshipFoo relationshipFoo1To2 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipFoo1To2);

    RelationshipFoo relationshipFoo2o3 = new RelationshipFoo().setSource(urn2).setDestination(urn3);
    _writer.addRelationship(relationshipFoo2o3);

    // From urn1, get result with one hop, such as direct manager
    Filter sourceFilter = makeFilter("urn", urn1.toString());
    List<RecordTemplate> result =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 1, 1, 0, 10);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0), entity2);

    // get result with 2 hops, two managers
    List<RecordTemplate> result2 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 1, 2, 0, 10);
    assertEquals(result2.size(), 2);
    assertEquals(result2.get(0), entity2);
    assertEquals(result2.get(1), entity3);

    // get result with >= 3 hops, until end of the chain
    List<RecordTemplate> result3 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 1, 3, 0, 10);
    assertEquals(result3.size(), 2);
    assertEquals(result3.get(0), entity2);
    assertEquals(result3.get(1), entity3);

    // test dest filter, filter the list of result and only get urn3 back
    Filter destFilter = makeFilter("urn", urn3.toString());
    List<RecordTemplate> result4 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, destFilter, RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 1, 3, 0, 10);
    assertEquals(result4.size(), 1);
    assertEquals(result4.get(0), entity3);

    // test relationship filter: TODO: for all the interfaces add relationship filter testing

    // corner cases 1: minHops set to 3, get no result because the relationship chain reaches the end
    List<RecordTemplate> result5 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 3, 6, 0, 10);
    assertEquals(result5.size(), 0);

    // corner cases 2: minHops < maxHops, return no result
    List<RecordTemplate> result6 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 6, 0, 0, 10);
    assertEquals(result6.size(), 0);

    // test the relationship directions
    // From urn2, get result with one hop, such as direct manager.
    // NOTE: without direction specified in the matchTemplate, urn1 could end up in the result too
    Filter sourceFilter2 = makeFilter("urn", urn2.toString());
    List<RecordTemplate> result21 =
        _dao.findEntities(EntityFoo.class, sourceFilter2, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 1, 1, 0, 10);
    assertEquals(result21.size(), 1);
    assertEquals(result21.get(0), entity3);

    // get result with 2 hops, 1 manager
    List<RecordTemplate> result22 =
        _dao.findEntities(EntityFoo.class, sourceFilter2, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 1, 2, 0, 10);
    assertEquals(result22.size(), 1);
    assertEquals(result22.get(0), entity3);

    // let's see what we get if we use interface 1, it should return urn3 only
    List<RecordTemplate> resultInterface1 =
        _dao.findEntities(EntityFoo.class, sourceFilter2, null, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultInterface1.size(), 1);
    assertEquals(resultInterface1.get(0), entity3);

    // test INCOMING direction, use case such as who report to URN3
    Filter sourceFilter3 = makeFilter("urn", urn3.toString());
    List<RecordTemplate> resultIncoming =
        _dao.findEntities(EntityFoo.class, sourceFilter3, EntityFoo.class, emptyFilter(), RelationshipFoo.class,
            makeRelationshipFilter(emptyFilter(), RelationshipDirection.INCOMING), 1, 2, 0, 10);
    assertEquals(resultIncoming.size(), 2);
    assertEquals(resultIncoming.get(0), entity1);
    assertEquals(resultIncoming.get(1), entity2);
  }

  @Test
  public void testFindEntitiesViaTraversePathes() throws Exception {
    // Test interface 4
    Urn urn1 = makeUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("CorpGroup1");
    _writer.addEntity(entity1);

    Urn urn2 = makeUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("CorpUser2");
    _writer.addEntity(entity2);

    Urn urn3 = makeUrn(3);
    EntityBaz entity3 = new EntityBaz().setUrn(urn3).setValue("Dataset3");
    _writer.addEntity(entity3);

    // create relationship urn1 -> urn2 with RelationshipFoo (ex: CorpGroup1 HasMember CorpUser2)
    RelationshipFoo relationshipFoo1To2 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipFoo1To2);

    // create relationship urn2 -> urn3 with RelationshipBar (ex: Dataset3 is OwnedBy CorpUser2)
    RelationshipBar relationshipFoo2o3 = new RelationshipBar().setSource(urn3).setDestination(urn2);
    _writer.addRelationship(relationshipFoo2o3);

    // test source filter with urn
    Filter sourceFilter = makeFilter("urn", urn1.toString());

    // use case: return all the datasets owned by corpgroup1
    List paths = new ArrayList();
    paths.add(Triplet.with(RelationshipFoo.class, makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING),
        EntityBar.class));
    paths.add(Triplet.with(RelationshipBar.class, makeRelationshipFilter(emptyFilter(), RelationshipDirection.INCOMING),
        EntityBaz.class));
    List<RecordTemplate> result = _dao.findEntities(EntityFoo.class, sourceFilter, paths, 0, 10);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0), entity3);


    // use case: return all the datasets owned by corpgroup1
    List paths2 = new ArrayList();
    paths2.add(Triplet.with(RelationshipFoo.class, makeRelationshipFilter(emptyFilter(), RelationshipDirection.UNDIRECTED),
        EntityBar.class));
    paths2.add(Triplet.with(RelationshipBar.class, makeRelationshipFilter(emptyFilter(), RelationshipDirection.UNDIRECTED),
        EntityBaz.class));
    List<RecordTemplate> result2 = _dao.findEntities(EntityFoo.class, sourceFilter, paths2, 0, 10);
    assertEquals(result2, result);

    // add another user & dataset
    Urn urn4 = makeUrn(4);
    EntityBar entity4 = new EntityBar().setUrn(urn4).setValue("CorpUser4");
    _writer.addEntity(entity4);

    Urn urn5 = makeUrn(5);
    EntityBaz entity5 = new EntityBaz().setUrn(urn5).setValue("Dataset5");
    _writer.addEntity(entity5);

    // create relationship urn4 -> urn5 with RelationshipBar
    RelationshipBar relationshipFoo4o5 = new RelationshipBar().setSource(urn5).setDestination(urn4);
    _writer.addRelationship(relationshipFoo4o5);

    // create relationship urn1 -> urn4 with RelationshipFoo
    RelationshipFoo relationshipFoo1To4 = new RelationshipFoo().setSource(urn1).setDestination(urn4);
    _writer.addRelationship(relationshipFoo1To4);

    List paths3 = new ArrayList();
    paths3.add(
        Triplet.with(RelationshipFoo.class, makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING),
            EntityBar.class));
    paths3.add(
        Triplet.with(RelationshipBar.class, makeRelationshipFilter(emptyFilter(), RelationshipDirection.INCOMING),
            EntityBaz.class));
    List<RecordTemplate> result3 = _dao.findEntities(EntityFoo.class, sourceFilter, paths3, 0, 10);
    assertEquals(result3.size(), 2);
    assertEquals(result3.get(0), entity3);
    assertEquals(result3.get(1), entity5);

    // test nulls
    List paths4 = new ArrayList();
    paths4.add(Triplet.with(null, null, null));
    List<RecordTemplate> result4 = _dao.findEntities(EntityFoo.class, sourceFilter, paths4, 0, 10);
    assertEquals(result4.size(), 2);
    assertEquals(result4.get(0), entity2);
    assertEquals(result4.get(1), entity4);

    // test partial nulls with entity
    List paths5 = new ArrayList();
    paths5.add(
        Triplet.with(null, makeRelationshipFilter(emptyFilter(), RelationshipDirection.OUTGOING), EntityBar.class));
    List<RecordTemplate> result5 = _dao.findEntities(EntityFoo.class, sourceFilter, paths5, 0, 10);
    assertEquals(result5.size(), 2);
    assertEquals(result5.get(0), entity2);
    assertEquals(result5.get(1), entity4);

    // test partial nulls with relationship
    List paths6 = new ArrayList();
    paths6.add(Triplet.with(null, null, EntityBar.class));
    List<RecordTemplate> result6 = _dao.findEntities(EntityFoo.class, sourceFilter, paths6, 0, 10);
    assertEquals(result6.size(), 2);
    assertEquals(result6.get(0), entity2);
    assertEquals(result6.get(1), entity4);
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

  private RelationshipFilter makeRelationshipFilter(Filter filter, RelationshipDirection relationshipDirection) {
    return new RelationshipFilter().setCriteria(filter.getCriteria()).setDirection(relationshipDirection);
  }
}
