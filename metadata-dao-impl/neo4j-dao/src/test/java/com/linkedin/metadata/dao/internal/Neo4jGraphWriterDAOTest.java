package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.utils.Neo4jTestServerBuilder;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.EntityBar;
import com.linkedin.testing.TestUtils;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.neo4j.driver.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class Neo4jGraphWriterDAOTest {

  private Neo4jTestServerBuilder _serverBuilder;
  private Neo4jGraphWriterDAO _dao;

  @BeforeMethod
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _dao = new Neo4jGraphWriterDAO(
        GraphDatabase.driver(_serverBuilder.boltURI()),
        TestUtils.getAllTestEntities()
    );
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Test
  public void testAddRemoveEntity() throws Exception {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn).setValue("foo");

    _dao.addEntity(entity);
    Optional<Map<String, Object>> node = _dao.getNode(urn);
    assertEntityFoo(node.get(), entity);

    _dao.removeEntity(urn);
    node = _dao.getNode(urn);
    assertFalse(node.isPresent());
  }

  @Test
  public void testPartialUpdateEntity() throws Exception {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn);

    _dao.addEntity(entity);
    Optional<Map<String, Object>> node = _dao.getNode(urn);
    assertEntityFoo(node.get(), entity);

    // add value for optional field
    EntityFoo entity2 = new EntityFoo().setUrn(urn).setValue("IamTheSameEntity");
    _dao.addEntity(entity2);
    node = _dao.getNode(urn);
    assertEquals(_dao.getAllNodes(urn).size(), 1);
    assertEntityFoo(node.get(), entity2);

    // change value for optional field
    EntityFoo entity3 = new EntityFoo().setUrn(urn).setValue("ChangeValue");
    _dao.addEntity(entity3);
    node = _dao.getNode(urn);
    assertEquals(_dao.getAllNodes(urn).size(), 1);
    assertEntityFoo(node.get(), entity3);
  }

  @Test
  public void testAddRemoveEntities() throws Exception {
    EntityFoo entity1 = new EntityFoo().setUrn(makeFooUrn(1)).setValue("foo");
    EntityFoo entity2 = new EntityFoo().setUrn(makeFooUrn(2)).setValue("bar");
    EntityFoo entity3 = new EntityFoo().setUrn(makeFooUrn(3)).setValue("baz");
    List<EntityFoo> entities = Arrays.asList(entity1, entity2, entity3);

    _dao.addEntities(entities);
    assertEntityFoo(_dao.getNode(entity1.getUrn()).get(), entity1);
    assertEntityFoo(_dao.getNode(entity2.getUrn()).get(), entity2);
    assertEntityFoo(_dao.getNode(entity3.getUrn()).get(), entity3);

    _dao.removeEntities(Arrays.asList(entity1.getUrn(), entity3.getUrn()));
    assertFalse(_dao.getNode(entity1.getUrn()).isPresent());
    assertTrue(_dao.getNode(entity2.getUrn()).isPresent());
    assertFalse(_dao.getNode(entity3.getUrn()).isPresent());
  }

  @Test
  public void testAddRelationshipNodeNonExist() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    BarUrn urn2 = makeBarUrn(2);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);

    _dao.addRelationship(relationship, REMOVE_NONE);

    assertRelationshipFoo(_dao.getEdges(relationship), 1);
    assertEntityFoo(_dao.getNode(urn1).get(), new EntityFoo().setUrn(urn1));
    assertEntityBar(_dao.getNode(urn2).get(), new EntityBar().setUrn(urn2));
  }

  @Test
  public void testPartialUpdateEntityCreatedByRelationship() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);

    _dao.addRelationship(relationship, REMOVE_NONE);

    // Check if adding an entity with same urn and with label creates a new node
    _dao.addEntity(new EntityFoo().setUrn(urn1));
    assertEquals(_dao.getAllNodes(urn1).size(), 1);
  }

  @Test
  public void testAddRemoveRelationships() throws Exception {
    // Add entity1
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _dao.addEntity(entity1);
    assertEntityFoo(_dao.getNode(urn1).get(), entity1);

    // Add entity2
    BarUrn urn2 = makeBarUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _dao.addEntity(entity2);
    assertEntityBar(_dao.getNode(urn2).get(), entity2);

    // add relationship1 (urn1 -> urn2)
    RelationshipFoo relationship1 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _dao.addRelationship(relationship1, REMOVE_NONE);
    assertRelationshipFoo(_dao.getEdges(relationship1), 1);

    // add relationship1 again
    _dao.addRelationship(relationship1);
    assertRelationshipFoo(_dao.getEdges(relationship1), 1);

    // add relationship2 (urn1 -> urn3)
    Urn urn3 = makeUrn(3);
    RelationshipFoo relationship2 = new RelationshipFoo().setSource(urn1).setDestination(urn3);
    _dao.addRelationship(relationship2);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn1, RelationshipFoo.class), 2);

    // remove relationship1
    _dao.removeRelationship(relationship1);
    assertRelationshipFoo(_dao.getEdges(relationship1), 0);

    // remove relationship1 & relationship2
    _dao.removeRelationships(Arrays.asList(relationship1, relationship2));
    assertRelationshipFoo(_dao.getEdgesFromSource(urn1, RelationshipFoo.class), 0);
  }

  @Test
  public void testAddRelationshipRemoveAll() throws Exception {
    // Add entity1
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _dao.addEntity(entity1);
    assertEntityFoo(_dao.getNode(urn1).get(), entity1);

    // Add entity2
    BarUrn urn2 = makeBarUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _dao.addEntity(entity2);
    assertEntityBar(_dao.getNode(urn2).get(), entity2);

    // add relationship1 (urn1 -> urn2)
    RelationshipFoo relationship1 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _dao.addRelationship(relationship1, REMOVE_NONE);
    assertRelationshipFoo(_dao.getEdges(relationship1), 1);

    // add relationship2 (urn1 -> urn3), removeAll from source
    Urn urn3 = makeUrn(3);
    RelationshipFoo relationship2 = new RelationshipFoo().setSource(urn1).setDestination(urn3);
    _dao.addRelationship(relationship2, REMOVE_ALL_EDGES_FROM_SOURCE);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn1, RelationshipFoo.class), 1);

    // add relationship3 (urn4 -> urn3), removeAll from destination
    Urn urn4 = makeUrn(4);
    RelationshipFoo relationship3 = new RelationshipFoo().setSource(urn4).setDestination(urn3);
    _dao.addRelationship(relationship3, REMOVE_ALL_EDGES_TO_DESTINATION);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn1, RelationshipFoo.class), 0);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn4, RelationshipFoo.class), 1);

    // add relationship3 again without removal
    _dao.addRelationship(relationship3);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn4, RelationshipFoo.class), 1);

    // add relationship3 again, removeAll from source & destination
    _dao.addRelationship(relationship3, REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn1, RelationshipFoo.class), 0);
    assertRelationshipFoo(_dao.getEdgesFromSource(urn4, RelationshipFoo.class), 1);
  }

  @Test
  public void testGetNodeTypeFromUrn() {
    assertEquals(_dao.getNodeType(makeBarUrn(1)), ":`com.linkedin.testing.EntityBar`");
    assertEquals(_dao.getNodeType(makeFooUrn(1)), ":`com.linkedin.testing.EntityFoo`");
    assertEquals(_dao.getNodeType(makeUrn(1, "entityFoo")), ":`com.linkedin.testing.EntityFoo`");
    assertEquals(_dao.getNodeType(makeUrn("1")), ":UNKNOWN");

    // test consistency !!
    assertEquals(_dao.getNodeType(makeBarUrn(1)), getTypeOrEmptyString(EntityBar.class));
    assertEquals(_dao.getNodeType(makeFooUrn(1)), getTypeOrEmptyString(EntityFoo.class));
  }

  private void assertEntityFoo(@Nonnull Map<String, Object> node, @Nonnull EntityFoo entity) {
    assertEquals(node.get("urn"), entity.getUrn().toString());
    assertEquals(node.get("value"), entity.getValue());
  }

  private void assertEntityBar(@Nonnull Map<String, Object> node, @Nonnull EntityBar entity) {
    assertEquals(node.get("urn"), entity.getUrn().toString());
    assertEquals(node.get("value"), entity.getValue());
  }

  private void assertRelationshipFoo(@Nonnull List<Map<String, Object>> edges, int count) {
    assertEquals(edges.size(), count);
    edges.forEach(edge -> assertTrue(edge.isEmpty()));
  }
}
