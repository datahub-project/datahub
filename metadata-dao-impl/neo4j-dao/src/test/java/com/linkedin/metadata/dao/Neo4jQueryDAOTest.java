package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.metadata.utils.Neo4jTestServerBuilder;
import com.linkedin.testing.EntityBaz;
import com.linkedin.testing.RelationshipBar;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.EntityBar;
import com.linkedin.testing.TestUtils;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BazUrn;
import com.linkedin.testing.urn.FooUrn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.javatuples.Triplet;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.QueryUtils.*;
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
    _writer = new Neo4jGraphWriterDAO(driver, TestUtils.getAllTestEntities());
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Test
  public void testFindEntityByUrn() throws Exception {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn).setValue("foo");

    _writer.addEntity(entity);

    Filter filter = newFilter("urn", urn.toString());
    List<EntityFoo> found = _dao.findEntities(EntityFoo.class, filter, -1, -1);

    assertEquals(found.size(), 1);
    assertEquals(found.get(0), entity);
  }

  @Test
  public void testFindEntityByAttribute() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    FooUrn urn2 = makeFooUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo");
    _writer.addEntity(entity2);

    // find by removed
    Filter filter1 = newFilter("value", "foo");
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
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    Statement statement = new Statement("MATCH (n {value:\"foo\"}) RETURN n ORDER BY n.urn", Collections.emptyMap());
    List<EntityFoo> found = _dao.findEntities(EntityFoo.class, statement);
    assertEquals(found.size(), 1);
    assertEquals(found.get(0), entity1);

    FooUrn urn2 = makeFooUrn(2);
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
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo1");
    _writer.addEntity(entity1);

    FooUrn urn2 = makeFooUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo2");
    _writer.addEntity(entity2);

    FooUrn urn3 = makeFooUrn(3);
    EntityFoo entity3 = new EntityFoo().setUrn(urn3).setValue("foo3");
    _writer.addEntity(entity3);

    BarUrn urn4 = makeBarUrn(4);
    EntityBar entity4 = new EntityBar().setUrn(urn4).setValue("bar4");
    _writer.addEntity(entity4);

    BarUrn urn5 = makeBarUrn(5);
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
    Filter sourceFilter = newFilter("urn", urn2.toString());

    // use case: the direct reportee to me (urn2)
    List<RecordTemplate> resultIncoming =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 10);
    assertEquals(resultIncoming.size(), 1);
    assertEquals(resultIncoming.get(0), entity1);

    // use case: the manager I (urn2) am report to
    List<RecordTemplate> resultOutgoing =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultOutgoing.size(), 1);
    assertEquals(resultOutgoing.get(0), entity3);

    // use case: give me my friends at one degree/hop
    List<RecordTemplate> resultUndirected =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.UNDIRECTED), 0, 10);
    assertEquals(resultUndirected.size(), 2);
    assertEquals(resultUndirected.get(0), entity1);
    assertEquals(resultUndirected.get(1), entity3);

    // Test: if dest entity type is not specified, from urn1, it will return a mixed collection of entities like urn2 and urn4
    // urn5 should not be returned because it is based on RelationshipBar.
    sourceFilter = newFilter("urn", urn1.toString());
    List<RecordTemplate> resultNullDest =
        _dao.findEntities(EntityFoo.class, sourceFilter, null, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultNullDest.size(), 2);
    assertEquals(resultNullDest.get(0), entity2);
    assertEquals(resultNullDest.get(1), entity4);

    // Test: source filter on other attributes
    sourceFilter = newFilter("value", "foo2");
    List<RecordTemplate> result =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 10);
    assertEquals(result, resultIncoming);

    //Test relationship filters on the attributes
    Filter filter = newFilter("type", "apa");
    List<RecordTemplate> resultWithRelationshipFilter =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(filter, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultWithRelationshipFilter.size(), 1);
    assertEquals(resultWithRelationshipFilter.get(0), entity3);

    //Test a wrong value for relationship filters
    Filter relationshipFilterWrongValue = newFilter("type", "wrongValue");
    List<RecordTemplate> resultWithRelationshipFilter2 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(relationshipFilterWrongValue, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultWithRelationshipFilter2.size(), 0);
  }

  @Test
  public void testFindEntitiesMultiHops() throws Exception {
    // Test interface 5
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo1");
    _writer.addEntity(entity1);

    FooUrn urn2 = makeFooUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo2");
    _writer.addEntity(entity2);

    FooUrn urn3 = makeFooUrn(3);
    EntityFoo entity3 = new EntityFoo().setUrn(urn3).setValue("foo3");
    _writer.addEntity(entity3);

    // create relationship urn1 -> urn2 -> urn3 (use case: ReportTo list)
    RelationshipFoo relationshipFoo1To2 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipFoo1To2);

    RelationshipFoo relationshipFoo2o3 = new RelationshipFoo().setSource(urn2).setDestination(urn3);
    _writer.addRelationship(relationshipFoo2o3);

    // From urn1, get result with one hop, such as direct manager
    Filter sourceFilter = newFilter("urn", urn1.toString());
    List<RecordTemplate> result =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 1, 1, 0, 10);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0), entity2);

    // get result with 2 hops, two managers
    List<RecordTemplate> result2 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 1, 2, 0, 10);
    assertEquals(result2.size(), 2);
    assertEquals(result2.get(0), entity2);
    assertEquals(result2.get(1), entity3);

    // get result with >= 3 hops, until end of the chain
    List<RecordTemplate> result3 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 1, 3, 0, 10);
    assertEquals(result3.size(), 2);
    assertEquals(result3.get(0), entity2);
    assertEquals(result3.get(1), entity3);

    // test dest filter, filter the list of result and only get urn3 back
    Filter destFilter = newFilter("urn", urn3.toString());
    List<RecordTemplate> result4 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, destFilter, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 1, 3, 0, 10);
    assertEquals(result4.size(), 1);
    assertEquals(result4.get(0), entity3);

    // test relationship filter: TODO: for all the interfaces add relationship filter testing

    // corner cases 1: minHops set to 3, get no result because the relationship chain reaches the end
    List<RecordTemplate> result5 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 3, 6, 0, 10);
    assertEquals(result5.size(), 0);

    // corner cases 2: minHops < maxHops, return no result
    List<RecordTemplate> result6 =
        _dao.findEntities(EntityFoo.class, sourceFilter, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 6, 0, 0, 10);
    assertEquals(result6.size(), 0);

    // test the relationship directions
    // From urn2, get result with one hop, such as direct manager.
    // NOTE: without direction specified in the matchTemplate, urn1 could end up in the result too
    Filter sourceFilter2 = newFilter("urn", urn2.toString());
    List<RecordTemplate> result21 =
        _dao.findEntities(EntityFoo.class, sourceFilter2, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 1, 1, 0, 10);
    assertEquals(result21.size(), 1);
    assertEquals(result21.get(0), entity3);

    // get result with 2 hops, 1 manager
    List<RecordTemplate> result22 =
        _dao.findEntities(EntityFoo.class, sourceFilter2, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 1, 2, 0, 10);
    assertEquals(result22.size(), 1);
    assertEquals(result22.get(0), entity3);

    // let's see what we get if we use interface 1, it should return urn3 only
    List<RecordTemplate> resultInterface1 =
        _dao.findEntities(EntityFoo.class, sourceFilter2, null, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 0, 10);
    assertEquals(resultInterface1.size(), 1);
    assertEquals(resultInterface1.get(0), entity3);

    // test INCOMING direction, use case such as who report to URN3
    Filter sourceFilter3 = newFilter("urn", urn3.toString());
    List<RecordTemplate> resultIncoming =
        _dao.findEntities(EntityFoo.class, sourceFilter3, EntityFoo.class, EMPTY_FILTER, RelationshipFoo.class,
            createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 1, 2, 0, 10);
    assertEquals(resultIncoming.size(), 2);
    assertEquals(resultIncoming.get(0), entity2);
    assertEquals(resultIncoming.get(1), entity1);
  }

  @Test
  public void testFindEntitiesViaTraversePathes() throws Exception {
    // Test interface 4
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("CorpGroup1");
    _writer.addEntity(entity1);

    BarUrn urn2 = makeBarUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("CorpUser2");
    _writer.addEntity(entity2);

    BazUrn urn3 = makeBazUrn(3);
    EntityBaz entity3 = new EntityBaz().setUrn(urn3).setValue("Dataset3");
    _writer.addEntity(entity3);

    // create relationship urn1 -> urn2 with RelationshipFoo (ex: CorpGroup1 HasMember CorpUser2)
    RelationshipFoo relationshipFoo1To2 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationshipFoo1To2);

    // create relationship urn2 -> urn3 with RelationshipBar (ex: Dataset3 is OwnedBy CorpUser2)
    RelationshipBar relationshipFoo2o3 = new RelationshipBar().setSource(urn3).setDestination(urn2);
    _writer.addRelationship(relationshipFoo2o3);

    // test source filter with urn
    Filter sourceFilter = newFilter("urn", urn1.toString());

    // use case: return all the datasets owned by corpgroup1
    List paths = new ArrayList();
    paths.add(
        Triplet.with(RelationshipFoo.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
            EntityBar.class));
    paths.add(
        Triplet.with(RelationshipBar.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            EntityBaz.class));
    List<RecordTemplate> result = _dao.findEntities(EntityFoo.class, sourceFilter, paths, 0, 10);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0), entity3);

    // use case: return all the datasets owned by corpgroup1
    List paths2 = new ArrayList();
    paths2.add(
        Triplet.with(RelationshipFoo.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.UNDIRECTED),
            EntityBar.class));
    paths2.add(
        Triplet.with(RelationshipBar.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.UNDIRECTED),
            EntityBaz.class));
    List<RecordTemplate> result2 = _dao.findEntities(EntityFoo.class, sourceFilter, paths2, 0, 10);
    assertEquals(result2, result);

    // add another user & dataset
    BarUrn urn4 = makeBarUrn(4);
    EntityBar entity4 = new EntityBar().setUrn(urn4).setValue("CorpUser4");
    _writer.addEntity(entity4);

    BazUrn urn5 = makeBazUrn(5);
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
        Triplet.with(RelationshipFoo.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
            EntityBar.class));
    paths3.add(
        Triplet.with(RelationshipBar.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            EntityBaz.class));
    List<RecordTemplate> result3 = _dao.findEntities(EntityFoo.class, sourceFilter, paths3, 0, 10);
    assertEquals(result3.size(), 2);
    assertEquals(result3.get(0), entity5);
    assertEquals(result3.get(1), entity3);

    // test nulls
    List paths4 = new ArrayList();
    paths4.add(Triplet.with(null, null, null));
    List<RecordTemplate> result4 = _dao.findEntities(EntityFoo.class, sourceFilter, paths4, 0, 10);
    assertEquals(result4.size(), 2);
    assertEquals(result4.get(0), entity4);
    assertEquals(result4.get(1), entity2);

    // test partial nulls with entity
    List paths5 = new ArrayList();
    paths5.add(
        Triplet.with(null, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), EntityBar.class));
    List<RecordTemplate> result5 = _dao.findEntities(EntityFoo.class, sourceFilter, paths5, 0, 10);
    assertEquals(result5.size(), 2);
    assertEquals(result5.get(0), entity4);
    assertEquals(result5.get(1), entity2);

    // test partial nulls with relationship
    List paths6 = new ArrayList();
    paths6.add(Triplet.with(null, null, EntityBar.class));
    List<RecordTemplate> result6 = _dao.findEntities(EntityFoo.class, sourceFilter, paths6, 0, 10);
    assertEquals(result6.size(), 2);
    assertEquals(result6.get(0), entity4);
    assertEquals(result6.get(1), entity2);
  }

  @Test
  public void testFindRelationship() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    BarUrn urn2 = makeBarUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _writer.addEntity(entity2);

    // create relationship urn1 -> urn2
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _writer.addRelationship(relationship);

    // find by source
    Filter filter1 = newFilter("urn", urn1.toString());
    List<RelationshipFoo> found =
        _dao.findRelationshipsFromSource(null, filter1, RelationshipFoo.class, EMPTY_FILTER, -1, -1);
    assertEquals(found, Collections.singletonList(relationship));

    // find by destination
    Filter filter2 = newFilter("urn", urn2.toString());
    found =
        _dao.findRelationshipsFromDestination(EntityBar.class, filter2, RelationshipFoo.class, EMPTY_FILTER, -1, -1);
    assertEquals(found, Collections.singletonList(relationship));

    // find by source and destination
    found = _dao.findRelationships(null, filter1, null, filter2, RelationshipFoo.class, EMPTY_FILTER, 0, 10);
    assertEquals(found, Collections.singletonList(relationship));
  }

  @Test
  public void testFindRelationshipByQuery() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _writer.addEntity(entity1);

    BarUrn urn2 = makeBarUrn(2);
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

  @Test
  public void testFindNodesInPath() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("fooDirector");
    _writer.addEntity(entity1);

    FooUrn urn2 = makeFooUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("fooManager1");
    _writer.addEntity(entity2);

    FooUrn urn3 = makeFooUrn(3);
    EntityFoo entity3 = new EntityFoo().setUrn(urn3).setValue("fooManager2");
    _writer.addEntity(entity3);

    FooUrn urn4 = makeFooUrn(4);
    EntityFoo entity4 = new EntityFoo().setUrn(urn3).setValue("fooReport1ofManager1");
    _writer.addEntity(entity4);

    FooUrn urn5 = makeFooUrn(5);
    EntityFoo entity5 = new EntityFoo().setUrn(urn3).setValue("fooReport1ofManager2");
    _writer.addEntity(entity5);

    FooUrn urn6 = makeFooUrn(6);
    EntityFoo entity6 = new EntityFoo().setUrn(urn3).setValue("fooReport2ofManager2");
    _writer.addEntity(entity6);

    // Create relationships - simulate reportsto use case
    _writer.addRelationship(new RelationshipFoo().setSource(urn6).setDestination(urn3));
    _writer.addRelationship(new RelationshipFoo().setSource(urn5).setDestination(urn3));
    _writer.addRelationship(new RelationshipFoo().setSource(urn4).setDestination(urn2));
    _writer.addRelationship(new RelationshipFoo().setSource(urn3).setDestination(urn1));
    _writer.addRelationship(new RelationshipFoo().setSource(urn2).setDestination(urn1));

    // Get reports roll-up - 2 levels
    Filter sourceFilter = newFilter("urn", urn1.toString());
    RelationshipFilter relationshipFilter = createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING);
    List<List<RecordTemplate>> paths = _dao.getTraversedPaths(EntityFoo.class, sourceFilter, null,
        EMPTY_FILTER, RelationshipFoo.class, relationshipFilter, 1, 2, -1, -1);
    assertEquals(paths.size(), 5);
    assertEquals(paths.stream().filter(l -> l.size() == 3).collect(Collectors.toList()).size(), 2);
    assertEquals(paths.stream().filter(l -> l.size() == 5).collect(Collectors.toList()).size(), 3);

    // Get reports roll-up - 1 level
    paths = _dao.getTraversedPaths(EntityFoo.class, sourceFilter, null,
        EMPTY_FILTER, RelationshipFoo.class, relationshipFilter, 1, 1, -1, -1);
    assertEquals(paths.size(), 2);
    assertEquals(paths.stream().filter(l -> l.size() == 3).collect(Collectors.toList()).size(), 2);
    assertEquals(paths.stream().filter(l -> l.size() == 5).collect(Collectors.toList()).size(), 0);
  }
}
