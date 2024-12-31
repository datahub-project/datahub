package com.linkedin.common.util;

import static com.datahub.utils.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.test.testing.AspectBar;
import com.datahub.test.testing.AspectFoo;
import com.datahub.test.testing.DeltaUnion;
import com.datahub.test.testing.DeltaUnionAlias;
import com.datahub.test.testing.EntityAspectUnion;
import com.datahub.test.testing.EntityAspectUnionAlias;
import com.datahub.test.testing.EntityAspectUnionAliasArray;
import com.datahub.test.testing.EntityAspectUnionArray;
import com.datahub.test.testing.EntityBar;
import com.datahub.test.testing.EntityDelta;
import com.datahub.test.testing.EntityDeltaAlias;
import com.datahub.test.testing.EntityDocument;
import com.datahub.test.testing.EntityFoo;
import com.datahub.test.testing.EntitySnapshot;
import com.datahub.test.testing.EntitySnapshotAlias;
import com.datahub.test.testing.EntityUnion;
import com.datahub.test.testing.EntityUnionAlias;
import com.datahub.test.testing.InvalidAspectUnion;
import com.datahub.test.testing.RelationshipFoo;
import com.datahub.test.testing.RelationshipUnion;
import com.datahub.test.testing.RelationshipUnionAlias;
import com.datahub.test.testing.SnapshotUnion;
import com.datahub.test.testing.SnapshotUnionAlias;
import com.datahub.test.testing.urn.BarUrn;
import com.datahub.test.testing.urn.FooUrn;
import com.datahub.test.testing.urn.PizzaUrn;
import com.datahub.util.ModelUtils;
import com.datahub.util.exception.InvalidSchemaException;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class ModelUtilsTest {

  class ChildUrn extends Urn {

    public ChildUrn(String rawUrn) throws URISyntaxException {
      super(rawUrn);
    }
  }

  @Test
  public void testGetAspectName() {
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    assertEquals(aspectName, AspectFoo.class.getCanonicalName());
  }

  @Test
  public void testGetAspectClass() {
    Class aspectClass = ModelUtils.getAspectClass(AspectFoo.class.getCanonicalName());
    assertEquals(aspectClass, AspectFoo.class);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetInvalidAspectClass() {
    ModelUtils.getAspectClass(EntityAspectUnion.class.getCanonicalName());
  }

  @Test
  public void testGetValidAspectTypes() {
    Set<Class<? extends RecordTemplate>> validTypes =
        ModelUtils.getValidAspectTypes(EntityAspectUnion.class);

    assertEquals(validTypes, ImmutableSet.of(AspectFoo.class, AspectBar.class));
  }

  @Test
  public void testGetValidSnapshotClassFromName() {
    Class<? extends RecordTemplate> actualClass =
        ModelUtils.getMetadataSnapshotClassFromName(EntitySnapshot.class.getCanonicalName());
    assertEquals(actualClass, EntitySnapshot.class);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidSnapshotClassFromName() {
    ModelUtils.getMetadataSnapshotClassFromName(AspectFoo.class.getCanonicalName());
  }

  @Test
  public void testGetUrnFromSnapshot() {
    Urn expected = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromSnapshot(snapshot);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromSnapshotUnion() {
    Urn expected = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(expected);
    SnapshotUnion snapshotUnion = new SnapshotUnion();
    snapshotUnion.setEntitySnapshot(snapshot);

    Urn urn = ModelUtils.getUrnFromSnapshotUnion(snapshotUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromSnapshotUnionAlias() {
    Urn expected = makeUrn(1);
    EntitySnapshotAlias snapshot = new EntitySnapshotAlias().setUrn(expected);
    SnapshotUnionAlias snapshotUnion = new SnapshotUnionAlias();
    snapshotUnion.setEntity(snapshot);

    Urn urn = ModelUtils.getUrnFromSnapshotUnion(snapshotUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDelta() {
    Urn expected = makeUrn(1);
    EntityDelta delta = new EntityDelta().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromDelta(delta);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDeltaUnion() {
    Urn expected = makeUrn(1);
    EntityDelta delta = new EntityDelta().setUrn(expected);
    DeltaUnion deltaUnion = new DeltaUnion();
    deltaUnion.setEntityDelta(delta);

    Urn urn = ModelUtils.getUrnFromDeltaUnion(deltaUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDeltaUnionAlias() {
    Urn expected = makeUrn(1);
    EntityDeltaAlias delta = new EntityDeltaAlias().setUrn(expected);
    DeltaUnionAlias deltaUnion = new DeltaUnionAlias();
    deltaUnion.setEntity(delta);

    Urn urn = ModelUtils.getUrnFromDeltaUnion(deltaUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDocument() {
    Urn expected = makeUrn(1);
    EntityDocument document = new EntityDocument().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromDocument(document);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromEntity() {
    FooUrn expected = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromEntity(entity);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromRelationship() {
    FooUrn expectedSource = makeFooUrn(1);
    BarUrn expectedDestination = makeBarUrn(1);
    RelationshipFoo relationship =
        new RelationshipFoo().setSource(expectedSource).setDestination(expectedDestination);

    Urn sourceUrn = ModelUtils.getSourceUrnFromRelationship(relationship);
    Urn destinationUrn = ModelUtils.getDestinationUrnFromRelationship(relationship);
    assertEquals(sourceUrn, expectedSource);
    assertEquals(destinationUrn, expectedDestination);
  }

  @Test
  public void testGetAspectsFromSnapshot() throws IOException {
    EntitySnapshot snapshot = new EntitySnapshot();
    snapshot.setAspects(new EntityAspectUnionArray());
    snapshot.getAspects().add(new EntityAspectUnion());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setAspectFoo(foo);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(snapshot);

    assertEquals(aspects.size(), 1);
    assertEquals(aspects.get(0), foo);
  }

  @Test
  public void testGetAspectsFromSnapshotAlias() throws IOException {
    EntitySnapshotAlias snapshot = new EntitySnapshotAlias();
    snapshot.setAspects(new EntityAspectUnionAliasArray());
    AspectFoo foo = new AspectFoo();
    EntityAspectUnionAlias aspect1 = new EntityAspectUnionAlias();
    aspect1.setFoo(foo);
    snapshot.getAspects().add(aspect1);
    AspectBar bar = new AspectBar();
    EntityAspectUnionAlias aspect2 = new EntityAspectUnionAlias();
    aspect2.setBar(bar);
    snapshot.getAspects().add(aspect2);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(snapshot);

    assertEquals(aspects.size(), 2);
    assertEquals(aspects.get(0), foo);
    assertEquals(aspects.get(1), bar);
  }

  @Test
  public void testGetAspectFromSnapshot() throws IOException {
    EntitySnapshot snapshot = new EntitySnapshot();
    snapshot.setAspects(new EntityAspectUnionArray());
    snapshot.getAspects().add(new EntityAspectUnion());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setAspectFoo(foo);

    Optional<AspectFoo> aspectFoo = ModelUtils.getAspectFromSnapshot(snapshot, AspectFoo.class);
    assertTrue(aspectFoo.isPresent());
    assertEquals(aspectFoo.get(), foo);

    Optional<AspectBar> aspectBar = ModelUtils.getAspectFromSnapshot(snapshot, AspectBar.class);
    assertFalse(aspectBar.isPresent());
  }

  @Test
  public void testGetAspectsFromSnapshotUnion() throws IOException {
    EntitySnapshot snapshot = new EntitySnapshot();
    snapshot.setAspects(new EntityAspectUnionArray());
    snapshot.getAspects().add(new EntityAspectUnion());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setAspectFoo(foo);
    SnapshotUnion snapshotUnion = new SnapshotUnion();
    snapshotUnion.setEntitySnapshot(snapshot);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshotUnion(snapshotUnion);

    assertEquals(aspects.size(), 1);
    assertEquals(aspects.get(0), foo);
  }

  @Test
  public void testGetAspectsFromSnapshotUnionAlias() throws IOException {
    EntitySnapshotAlias snapshot = new EntitySnapshotAlias();
    snapshot.setAspects(new EntityAspectUnionAliasArray());
    snapshot.getAspects().add(new EntityAspectUnionAlias());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setFoo(foo);
    SnapshotUnionAlias snapshotUnion = new SnapshotUnionAlias();
    snapshotUnion.setEntity(snapshot);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshotUnion(snapshotUnion);

    assertEquals(aspects.size(), 1);
    assertEquals(aspects.get(0), foo);
  }

  @Test
  public void testNewSnapshot() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    EntityAspectUnion aspectUnion = new EntityAspectUnion();
    aspectUnion.setAspectFoo(foo);

    EntitySnapshot snapshot =
        ModelUtils.newSnapshot(EntitySnapshot.class, urn, Lists.newArrayList(aspectUnion));

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(snapshot.getAspects().size(), 1);
    assertEquals(snapshot.getAspects().get(0).getAspectFoo(), foo);
  }

  @Test
  public void testNewAspect() {
    AspectFoo foo = new AspectFoo().setValue("foo");

    EntityAspectUnion aspectUnion = ModelUtils.newAspectUnion(EntityAspectUnion.class, foo);

    assertEquals(aspectUnion.getAspectFoo(), foo);
  }

  @Test
  public void testNewAspectAlias() {
    AspectFoo foo = new AspectFoo().setValue("foo");

    EntityAspectUnionAlias aspectUnion =
        ModelUtils.newAspectUnion(EntityAspectUnionAlias.class, foo);

    assertEquals(aspectUnion.getFoo(), foo);
  }

  @Test
  public void testAspectClassForSnapshot() {
    assertEquals(ModelUtils.aspectClassForSnapshot(EntitySnapshot.class), EntityAspectUnion.class);
  }

  @Test
  public void testUrnClassForEntity() {
    assertEquals(ModelUtils.urnClassForEntity(EntityBar.class), BarUrn.class);
  }

  @Test
  public void testUrnClassForSnapshot() {
    assertEquals(ModelUtils.urnClassForSnapshot(EntitySnapshot.class), Urn.class);
  }

  @Test
  public void testUrnClassForDelta() {
    assertEquals(ModelUtils.urnClassForDelta(EntityDelta.class), Urn.class);
  }

  @Test
  public void testUrnClassForDocument() {
    assertEquals(ModelUtils.urnClassForDocument(EntityDocument.class), Urn.class);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testValidateIncorrectAspectForSnapshot() {
    ModelUtils.validateSnapshotAspect(EntitySnapshot.class, InvalidAspectUnion.class);
  }

  @Test
  public void testValidateCorrectAspectForSnapshot() {
    ModelUtils.validateSnapshotAspect(EntitySnapshot.class, EntityAspectUnion.class);
  }

  @Test
  public void testValidateCorrectUrnForSnapshot() {
    ModelUtils.validateSnapshotUrn(EntitySnapshot.class, Urn.class);
    ModelUtils.validateSnapshotUrn(EntitySnapshot.class, ChildUrn.class);
  }

  @Test
  public void testNewRelatioshipUnion() {
    RelationshipFoo foo =
        new RelationshipFoo().setDestination(makeFooUrn(1)).setSource(makeFooUrn(2));

    RelationshipUnion relationshipUnion =
        ModelUtils.newRelationshipUnion(RelationshipUnion.class, foo);

    assertEquals(relationshipUnion.getRelationshipFoo(), foo);
  }

  @Test
  public void testNewRelatioshipUnionAlias() {
    RelationshipFoo foo =
        new RelationshipFoo().setDestination(makeFooUrn(1)).setSource(makeFooUrn(2));

    RelationshipUnionAlias relationshipUnion =
        ModelUtils.newRelationshipUnion(RelationshipUnionAlias.class, foo);

    assertEquals(relationshipUnion.getFoo(), foo);
  }

  @Test
  public void testGetMAETopicName() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");

    assertEquals(
        ModelUtils.getAspectSpecificMAETopicName(urn, foo), "METADATA_AUDIT_EVENT_FOO_ASPECTFOO");

    PizzaUrn pizza = new PizzaUrn(1);
    AspectBar bar = new AspectBar().setValue("bar");
    assertEquals(
        ModelUtils.getAspectSpecificMAETopicName(pizza, bar),
        "METADATA_AUDIT_EVENT_PIZZA_ASPECTBAR");
  }

  @Test
  public void testNewEntityUnion() {
    EntityFoo entityFoo = new EntityFoo().setUrn(makeFooUrn(1));
    EntityUnion entityUnion = ModelUtils.newEntityUnion(EntityUnion.class, entityFoo);

    assertEquals(entityUnion.getEntityFoo(), entityFoo);
  }

  @Test
  public void testNewEntityUnionAlias() {
    EntityFoo entityFoo = new EntityFoo().setUrn(makeFooUrn(1));
    EntityUnionAlias entityUnion = ModelUtils.newEntityUnion(EntityUnionAlias.class, entityFoo);

    assertEquals(entityUnion.getFoo(), entityFoo);
  }
}
