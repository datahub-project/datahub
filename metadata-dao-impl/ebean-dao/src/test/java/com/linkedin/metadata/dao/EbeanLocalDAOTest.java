package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.equality.AlwaysFalseEqualityTester;
import com.linkedin.metadata.dao.equality.DefaultEqualityTester;
import com.linkedin.metadata.dao.exception.InvalidMetadataType;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.utils.BarUrnPathExtractor;
import com.linkedin.metadata.dao.utils.BazUrnPathExtractor;
import com.linkedin.metadata.dao.utils.FooUrnPathExtractor;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexPathParams;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectBaz;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.AspectFooEvolved;
import com.linkedin.testing.AspectInvalid;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BazUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.Transaction;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.persistence.RollbackException;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class EbeanLocalDAOTest {

  private EbeanServer _server;
  private BaseMetadataEventProducer _mockProducer;
  private AuditStamp _dummyAuditStamp;

  @BeforeMethod
  public void setupTest() {
    _server = EbeanServerFactory.create(EbeanLocalDAO.createTestingH2ServerConfig());
    _mockProducer = mock(BaseMetadataEventProducer.class);
    _dummyAuditStamp = makeAuditStamp("foo", 1234);
  }

  @Test(expectedExceptions = InvalidMetadataType.class)
  public void testMetadataAspectCheck() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);

    dao.add(makeFooUrn(1), new AspectInvalid().setValue("invalid"), _dummyAuditStamp);
  }

  @Test
  public void testAddOne() {
    Clock mockClock = mock(Clock.class);
    when(mockClock.millis()).thenReturn(1234L);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.setClock(mockClock);
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo expected = new AspectFoo().setValue("foo");
    CorpuserUrn actor = new CorpuserUrn("actor");
    CorpuserUrn impersonator = new CorpuserUrn("impersonator");

    dao.add(urn, expected, makeAuditStamp(actor, impersonator, 1234));

    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);

    assertNotNull(aspect);
    assertEquals(aspect.getKey().getUrn(), urn.toString());
    assertEquals(aspect.getKey().getAspect(), aspectName);
    assertEquals(aspect.getKey().getVersion(), 0);
    assertEquals(aspect.getCreatedOn(), new Timestamp(1234));
    assertEquals(aspect.getCreatedBy(), "urn:li:corpuser:actor");
    assertEquals(aspect.getCreatedFor(), "urn:li:corpuser:impersonator");

    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, expected);

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, expected);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testAddTwo() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    AspectFoo v0 = new AspectFoo().setValue("bar");

    dao.add(urn, v1, _dummyAuditStamp);
    dao.add(urn, v0, _dummyAuditStamp);

    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, v0);

    aspect = getMetadata(urn, aspectName, 1);
    actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, v1);

    InOrder inOrder = inOrder(_mockProducer);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, v1);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, v1, v0);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testDefaultEqualityTester() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.setEqualityTester(AspectFoo.class, DefaultEqualityTester.<AspectFoo>newInstance());
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");

    dao.add(urn, foo, _dummyAuditStamp);
    dao.add(urn, foo, _dummyAuditStamp);
    dao.add(urn, bar, _dummyAuditStamp);

    // v0: bar
    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, bar);

    // v1: foo
    aspect = getMetadata(urn, aspectName, 1);
    actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, foo);

    // no v2
    assertNull(getMetadata(urn, aspectName, 2));

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, foo, bar);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testAlwaysFalseEqualityTester() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.setEqualityTester(AspectFoo.class, AlwaysFalseEqualityTester.<AspectFoo>newInstance());
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo foo1 = new AspectFoo().setValue("foo");
    AspectFoo foo2 = new AspectFoo().setValue("foo");

    dao.add(urn, foo1, _dummyAuditStamp);
    dao.add(urn, foo2, _dummyAuditStamp);

    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, foo1);

    aspect = getMetadata(urn, aspectName, 1);
    actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, foo2);

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, foo1, foo2);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testVersionBasedRetention() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.setRetention(AspectFoo.class, new VersionBasedRetention(2));
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v0 = new AspectFoo().setValue("baz");
    AspectFoo v1 = new AspectFoo().setValue("bar");
    AspectFoo v2 = new AspectFoo().setValue("foo");

    dao.add(urn, v1, _dummyAuditStamp);
    dao.add(urn, v2, _dummyAuditStamp);
    dao.add(urn, v0, _dummyAuditStamp);

    assertNull(getMetadata(urn, aspectName, 1));
    assertNotNull(getMetadata(urn, aspectName, 2));
    assertNotNull(getMetadata(urn, aspectName, 0));
  }

  @Test
  public void testTimeBasedRetention() {
    Clock mockClock = mock(Clock.class);
    when(mockClock.millis())
        // Format
        .thenReturn(10L) // v1 age check
        .thenReturn(20L) // v2 age check
        .thenReturn(120L); // v3 age check

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.setClock(mockClock);
    dao.setRetention(AspectFoo.class, new TimeBasedRetention(100));
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v0 = new AspectFoo().setValue("baz");
    AspectFoo v1 = new AspectFoo().setValue("bar");
    AspectFoo v2 = new AspectFoo().setValue("foo");

    dao.add(urn, v1, makeAuditStamp("foo", 10));
    dao.add(urn, v2, makeAuditStamp("foo", 20));
    dao.add(urn, v0, makeAuditStamp("foo", 120));

    assertNull(getMetadata(urn, aspectName, 1));
    assertNotNull(getMetadata(urn, aspectName, 2));
    assertNotNull(getMetadata(urn, aspectName, 0));
  }

  @Test
  public void testAddSuccessAfterRetry() {
    EbeanServer server = mock(EbeanServer.class);
    Transaction mockTransaction = mock(Transaction.class);
    when(server.beginTransaction()).thenReturn(mockTransaction);
    when(server.find(any(), any())).thenReturn(null);
    doThrow(RollbackException.class).doNothing().when(server).insert(any(EbeanMetadataAspect.class));
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, server, FooUrn.class);

    dao.add(makeFooUrn(1), new AspectFoo().setValue("foo"), _dummyAuditStamp);
  }

  @Test(expectedExceptions = RetryLimitReached.class)
  public void testAddFailedAfterRetry() {
    EbeanServer server = mock(EbeanServer.class);
    Transaction mockTransaction = mock(Transaction.class);
    when(server.beginTransaction()).thenReturn(mockTransaction);
    when(server.find(any(), any())).thenReturn(null);
    doThrow(RollbackException.class).when(server).insert(any(EbeanMetadataAspect.class));
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, server, FooUrn.class);

    dao.add(makeFooUrn(1), new AspectFoo().setValue("foo"), _dummyAuditStamp);
  }

  @Test
  public void testGetNonExisting() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn);

    assertFalse(foo.isPresent());
  }

  @Test
  public void testGetCapsSensitivity() {
    final EbeanLocalDAO<EntityAspectUnion, Urn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, Urn.class);
    final Urn urnCaps = makeUrn("Dataset");
    final Urn urnLower = makeUrn("dataset");

    final AspectFoo v0 = new AspectFoo().setValue("baz");
    final AspectFoo v1 = new AspectFoo().setValue("foo");

    // expect v0 to be overwritten with v1
    dao.add(urnCaps, v0, _dummyAuditStamp);
    dao.add(urnLower, v1, _dummyAuditStamp);

    Optional<AspectFoo> caps = dao.get(AspectFoo.class, urnCaps);
    assertTrue(caps.isPresent());
    assertEquals(caps.get(), v1);

    Optional<AspectFoo> lower = dao.get(AspectFoo.class, urnLower);
    assertTrue(lower.isPresent());
    assertEquals(lower.get(), v1);
  }

  @Test
  public void testGetLatestVersion() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, v0);
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 1, v1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), v0);
  }

  @Test
  public void testGetSpecificVersion() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, v0);
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 1, v1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn, 1);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), v1);
  }

  @Test
  public void testGetMultipleAspects() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo fooV0 = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, fooV0);
    AspectFoo fooV1 = new AspectFoo().setValue("bar");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 1, fooV1);
    AspectBar barV0 = new AspectBar().setValue("bar");
    addMetadata(urn, AspectBar.class.getCanonicalName(), 0, barV0);

    Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> result =
        dao.get(new HashSet<>(Arrays.asList(AspectBar.class, AspectFoo.class)), urn);

    assertEquals(result.size(), 2);
    assertEquals(result.get(AspectFoo.class).get(), fooV0);
    assertEquals(result.get(AspectBar.class).get(), barV0);
  }

  @Test
  public void testGetMultipleAspectsForMultipleUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);

    // urn1 has both foo & bar
    FooUrn urn1 = makeFooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    addMetadata(urn1, AspectFoo.class.getCanonicalName(), 0, foo1);
    AspectBar bar1 = new AspectBar().setValue("bar1");
    addMetadata(urn1, AspectBar.class.getCanonicalName(), 0, bar1);

    // urn2 has only foo
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    addMetadata(urn2, AspectFoo.class.getCanonicalName(), 0, foo2);

    // urn3 has nothing
    FooUrn urn3 = makeFooUrn(3);

    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> result =
        dao.get(ImmutableSet.of(AspectFoo.class, AspectBar.class), ImmutableSet.of(urn1, urn2, urn3));

    assertEquals(result.size(), 3);
    assertEquals(result.get(urn1).get(AspectFoo.class).get(), foo1);
    assertEquals(result.get(urn1).get(AspectBar.class).get(), bar1);
    assertEquals(result.get(urn2).get(AspectFoo.class).get(), foo2);
    assertFalse(result.get(urn2).get(AspectBar.class).isPresent());
    assertFalse(result.get(urn3).get(AspectFoo.class).isPresent());
    assertFalse(result.get(urn3).get(AspectBar.class).isPresent());
  }

  @Test
  public void testBackfill() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    AspectFoo expected = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, expected);

    Optional<AspectFoo> foo = dao.backfill(AspectFoo.class, urn);

    assertEquals(foo.get(), expected);

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, expected, expected);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testLocalSecondaryIndexBackfill() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.setUrnPathExtractor(new FooUrnPathExtractor());

    FooUrn urn = makeFooUrn(1);
    AspectFoo expected = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, expected);

    // Check if backfilled: _writeToLocalSecondary = false
    dao.backfill(AspectFoo.class, urn);
    assertEquals(getAllRecordsFromLocalIndex(urn).size(), 0);

    // Check if backfilled: _writeToLocalSecondary = true
    dao.enableLocalSecondaryIndex(true);
    dao.backfill(AspectFoo.class, urn);
    List<EbeanMetadataIndex> fooRecords = getAllRecordsFromLocalIndex(urn);
    assertEquals(fooRecords.size(), 1);
    EbeanMetadataIndex fooRecord = fooRecords.get(0);
    assertEquals(fooRecord.getUrn(), urn.toString());
    assertEquals(fooRecord.getAspect(), FooUrn.class.getCanonicalName());
    assertEquals(fooRecord.getPath(), "/fooId");
    assertEquals(fooRecord.getLongVal().longValue(), 1L);
  }

  @Test
  public void testBackfillWithUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, aspectFoo);
      addMetadata(urn, AspectBar.class.getCanonicalName(), 0, aspectBar);
    });

    // Backfill single aspect for set of urns
    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfilledAspects =
        dao.backfill(Collections.singleton(AspectFoo.class), new HashSet<>(urns));
    for (Urn urn : urns) {
      RecordTemplate aspect = aspects.get(urn).get(AspectFoo.class);
      assertEquals(backfilledAspects.get(urn).get(AspectFoo.class).get(), aspect);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, aspect, aspect);
    }
    clearInvocations(_mockProducer);

    // Backfill set of aspects for a single urn
    backfilledAspects =
        dao.backfill(ImmutableSet.of(AspectFoo.class, AspectBar.class), Collections.singleton(urns.get(0)));
    for (Class<? extends RecordTemplate> clazz : aspects.get(urns.get(0)).keySet()) {
      RecordTemplate aspect = aspects.get(urns.get(0)).get(clazz);
      assertEquals(backfilledAspects.get(urns.get(0)).get(clazz).get(), aspect);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urns.get(0), aspect, aspect);
    }
    clearInvocations(_mockProducer);

    // Backfill set of aspects for set of urns
    backfilledAspects = dao.backfill(ImmutableSet.of(AspectFoo.class, AspectBar.class), new HashSet<>(urns));
    for (Urn urn : urns) {
      for (Class<? extends RecordTemplate> clazz : aspects.get(urn).keySet()) {
        RecordTemplate aspect = aspects.get(urn).get(clazz);
        assertEquals(backfilledAspects.get(urn).get(clazz).get(), aspect);
        verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, aspect, aspect);
      }
    }
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testBackfillUsingSCSI() {
    LocalDAOStorageConfig storageConfig =
        makeLocalDAOStorageConfig(AspectFoo.class, Collections.singletonList("/value"), AspectBar.class,
            Collections.singletonList("/value"));
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server, storageConfig, FooUrn.class);
    dao.enableLocalSecondaryIndex(true);

    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");

      // update metadata_aspects table
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, aspectFoo);
      addMetadata(urn, AspectBar.class.getCanonicalName(), 0, aspectBar);

      // only index urn
      addIndex(urn, FooUrn.class.getCanonicalName(), "/fooId", urn.getFooIdEntity());
    });

    // Backfill in SCSI_ONLY mode
    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfilledAspects =
        dao.backfill(BackfillMode.SCSI_ONLY, Collections.singleton(AspectFoo.class), FooUrn.class, null, 3);
    for (int index = 0; index < 3; index++) {
      Urn urn = urns.get(index);
      RecordTemplate aspect = aspects.get(urn).get(AspectFoo.class);
      assertEquals(backfilledAspects.get(urn).get(AspectFoo.class).get(), aspect);
      verify(_mockProducer, times(0)).produceMetadataAuditEvent(urn, aspect, aspect);
    }
    IndexFilter indexFilter = new IndexFilter().setCriteria(
        new IndexCriterionArray(new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName())));
    assertEquals(dao.listUrns(indexFilter, null, 3).getValues().size(), 3);

    // Backfill in MAE_ONLY mode
    backfilledAspects =
        dao.backfill(BackfillMode.MAE_ONLY, Collections.singleton(AspectBar.class), FooUrn.class, null, 3);
    for (int index = 0; index < 3; index++) {
      Urn urn = urns.get(index);
      RecordTemplate aspect = aspects.get(urn).get(AspectBar.class);
      assertEquals(backfilledAspects.get(urn).get(AspectBar.class).get(), aspect);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, aspect, aspect);
    }
    clearInvocations(_mockProducer);

    indexFilter = new IndexFilter().setCriteria(
        new IndexCriterionArray(new IndexCriterion().setAspect(AspectBar.class.getCanonicalName())));
    assertEquals(dao.listUrns(indexFilter, null, 3).getValues().size(), 0);

    // Backfill in BACKFILL_ALL mode
    backfilledAspects =
        dao.backfill(BackfillMode.BACKFILL_ALL, ImmutableSet.of(AspectBar.class), FooUrn.class, null, 3);
    for (int index = 0; index < 3; index++) {
      Urn urn = urns.get(index);
      RecordTemplate aspect = aspects.get(urn).get(AspectBar.class);
      assertEquals(backfilledAspects.get(urn).get(AspectBar.class).get(), aspect);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, aspect, aspect);
    }
    verifyNoMoreInteractions(_mockProducer);
    assertEquals(dao.listUrns(indexFilter, null, 3).getValues().size(), 3);
  }

  @Test
  public void testListVersions() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    List<Long> versions = new ArrayList<>();
    for (long i = 0; i < 6; i++) {
      AspectFoo foo = new AspectFoo().setValue("foo" + i);
      addMetadata(urn, AspectFoo.class.getCanonicalName(), i, foo);
      versions.add(i);
    }

    ListResult<Long> results = dao.listVersions(AspectFoo.class, urn, 0, 5);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 5);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), versions.subList(0, 5));

    // List last page
    results = dao.listVersions(AspectFoo.class, urn, 5, 10);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 10);
    assertEquals(results.getTotalPageCount(), 1);
    assertEquals(results.getValues(), versions.subList(5, 6));

    // List beyond last page
    results = dao.listVersions(AspectFoo.class, urn, 6, 1);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 6);
    assertEquals(results.getValues(), new ArrayList<>());
  }

  private static IndexCriterionArray makeIndexCriterionArray(int size) {
    List<IndexCriterion> criterionArrays = new ArrayList<>();
    IntStream.range(0, size).forEach(i -> criterionArrays.add(new IndexCriterion().setAspect("aspect" + i)));
    return new IndexCriterionArray(criterionArrays);
  }

  @Test
  void testListUrnsFromIndexManyFilters() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    dao.enableLocalSecondaryIndex(true);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect1 = "aspect1" + System.currentTimeMillis();
    String aspect2 = "aspect2" + System.currentTimeMillis();

    addIndex(urn1, aspect1, "/path1", true); // boolean
    addIndex(urn1, aspect1, "/path2", 1.534e2); // double
    addIndex(urn1, aspect1, "/path3", 123.4f); // float
    addIndex(urn1, aspect2, "/path4", 123); // int
    addIndex(urn1, aspect2, "/path5", 1234L); // long
    addIndex(urn1, aspect2, "/path6", "val"); // string
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect1, "/path1", true); // boolean
    addIndex(urn2, aspect1, "/path2", 1.534e2); // double
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    addIndex(urn3, aspect1, "/path1", true); // boolean
    addIndex(urn3, aspect1, "/path2", 1.534e2); // double
    addIndex(urn3, aspect1, "/path3", 123.4f); // float
    addIndex(urn3, aspect2, "/path4", 123); // int
    addIndex(urn3, aspect2, "/path5", 1234L); // long
    addIndex(urn3, aspect2, "/path6", "val"); // string
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    IndexValue indexValue1 = new IndexValue();
    indexValue1.setBoolean(true);
    IndexCriterion criterion1 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path1").setValue(indexValue1));
    IndexValue indexValue2 = new IndexValue();
    indexValue2.setDouble(1.534e2);
    IndexCriterion criterion2 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path2").setValue(indexValue2));
    IndexValue indexValue3 = new IndexValue();
    indexValue3.setFloat(123.4f);
    IndexCriterion criterion3 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path3").setValue(indexValue3));
    IndexValue indexValue4 = new IndexValue();
    indexValue4.setInt(123);
    IndexCriterion criterion4 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path4").setValue(indexValue4));
    IndexValue indexValue5 = new IndexValue();
    indexValue5.setLong(1234L);
    IndexCriterion criterion5 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path5").setValue(indexValue5));
    IndexValue indexValue6 = new IndexValue();
    indexValue6.setString("val");
    IndexCriterion criterion6 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path6").setValue(indexValue6));

    // cover CONDITION other than EQUAL
    // GREATER_THAN
    IndexValue indexValue7 = new IndexValue();
    indexValue7.setInt(100);
    IndexCriterion criterion7 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(
            new IndexPathParams().setPath("/path4").setValue(indexValue7).setCondition(Condition.GREATER_THAN));

    // GREATER_THAN_EQUAL_TO
    IndexValue indexValue8 = new IndexValue();
    indexValue8.setFloat(100.2f);
    IndexCriterion criterion8 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path3")
            .setValue(indexValue8)
            .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO));

    // LESS_THAN
    IndexValue indexValue9 = new IndexValue();
    indexValue9.setDouble(1.894e2);
    IndexCriterion criterion9 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path2").setValue(indexValue9).setCondition(Condition.LESS_THAN));

    // LESS_THAN_EQUAL_TO
    IndexValue indexValue10 = new IndexValue();
    indexValue10.setLong(1111L);
    IndexCriterion criterion10 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path5").setValue(indexValue10));

    // 1. with two filter conditions
    IndexCriterionArray indexCriterionArray1 = new IndexCriterionArray(Arrays.asList(criterion1, criterion2));
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(indexCriterionArray1);
    ListResult<FooUrn> urns1 = dao.listUrns(indexFilter1, null, 3);

    assertEquals(urns1.getValues(), Arrays.asList(urn1, urn2, urn3));
    assertEquals(urns1.getTotalCount(), 3);
    assertEquals(urns1.getTotalPageCount(), 1);
    assertEquals(urns1.getPageSize(), 3);
    assertFalse(urns1.isHavingMore());

    // 2. with two filter conditions, check if LIMIT is working as desired i.e. totalCount is more than the page size
    ListResult<FooUrn> urns2 = dao.listUrns(indexFilter1, null, 2);
    assertEquals(urns2.getValues(), Arrays.asList(urn1, urn2));
    assertEquals(urns2.getTotalCount(), 3);
    assertEquals(urns2.getTotalPageCount(), 2);
    assertEquals(urns2.getPageSize(), 2);
    assertTrue(urns2.isHavingMore());

    // 3. with six filter conditions covering all different data types that value can take
    IndexCriterionArray indexCriterionArray3 =
        new IndexCriterionArray(Arrays.asList(criterion1, criterion2, criterion3, criterion4, criterion5, criterion6));
    final IndexFilter indexFilter3 = new IndexFilter().setCriteria(indexCriterionArray3);
    ListResult<FooUrn> urns3 = dao.listUrns(indexFilter3, urn1, 5);
    assertEquals(urns3.getValues(), Collections.singletonList(urn3));
    assertEquals(urns3.getTotalCount(), 1);
    assertEquals(urns3.getTotalPageCount(), 1);
    assertEquals(urns3.getPageSize(), 5);
    assertFalse(urns3.isHavingMore());

    // 4. GREATER_THAN criterion
    IndexCriterionArray indexCriterionArray4 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion2, criterion3, criterion4, criterion5, criterion6, criterion7));
    final IndexFilter indexFilter4 = new IndexFilter().setCriteria(indexCriterionArray4);
    ListResult<FooUrn> urns4 = dao.listUrns(indexFilter4, null, 5);
    assertEquals(urns4.getValues(), Arrays.asList(urn1, urn3));
    assertEquals(urns4.getTotalCount(), 2);
    assertEquals(urns4.getTotalPageCount(), 1);
    assertEquals(urns4.getPageSize(), 5);
    assertFalse(urns4.isHavingMore());

    // 5. GREATER_THAN_EQUAL_TO criterion
    IndexCriterionArray indexCriterionArray5 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion2, criterion3, criterion4, criterion5, criterion6, criterion7, criterion8));
    final IndexFilter indexFilter5 = new IndexFilter().setCriteria(indexCriterionArray5);
    ListResult<FooUrn> urns5 = dao.listUrns(indexFilter5, null, 10);
    assertEquals(urns5.getValues(), Arrays.asList(urn1, urn3));
    assertEquals(urns5.getTotalCount(), 2);
    assertEquals(urns5.getTotalPageCount(), 1);
    assertEquals(urns5.getPageSize(), 10);
    assertFalse(urns5.isHavingMore());

    // 6. LESS_THAN criterion
    IndexCriterionArray indexCriterionArray6 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion3, criterion4, criterion5, criterion6, criterion7, criterion8, criterion9));
    final IndexFilter indexFilter6 = new IndexFilter().setCriteria(indexCriterionArray6);
    ListResult<FooUrn> urns6 = dao.listUrns(indexFilter6, urn1, 8);
    assertEquals(urns6.getValues(), Collections.singletonList(urn3));
    assertEquals(urns6.getTotalCount(), 1);
    assertEquals(urns6.getTotalPageCount(), 1);
    assertEquals(urns6.getPageSize(), 8);
    assertFalse(urns6.isHavingMore());

    // 7. LESS_THAN_EQUAL_TO
    IndexCriterionArray indexCriterionArray7 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion3, criterion4, criterion5, criterion6, criterion7, criterion8, criterion9,
            criterion10));
    final IndexFilter indexFilter7 = new IndexFilter().setCriteria(indexCriterionArray7);
    ListResult<FooUrn> urns7 = dao.listUrns(indexFilter7, null, 4);
    assertEquals(urns7.getValues(), Collections.emptyList());
    assertEquals(urns7.getTotalCount(), 0);
    assertEquals(urns7.getTotalPageCount(), 0);
    assertEquals(urns7.getPageSize(), 4);
    assertFalse(urns7.isHavingMore());
  }

  @Test
  public void testListUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    AspectFoo foo = new AspectFoo().setValue("foo");
    List<FooUrn> urns = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);
      for (int j = 0; j < 3; j++) {
        addMetadata(urn, AspectFoo.class.getCanonicalName(), j, foo);
      }
      urns.add(urn);
    }

    ListResult<FooUrn> results = dao.listUrns(AspectFoo.class, 0, 1);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 1);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 3);
    assertEquals(results.getValues(), urns.subList(0, 1));

    // List next page
    results = dao.listUrns(AspectFoo.class, 1, 1);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 2);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 3);
    assertEquals(results.getValues(), urns.subList(1, 2));

    // Test List result sorted by Urns
    results = dao.listUrns(AspectFoo.class, 0, 5);
    assertEquals(results.getValues().size(), 3);
    assertEquals(results.getValues(), urns.subList(0, 3));
    assertEquals(results.getValues().get(0), makeFooUrn(0));
    assertEquals(results.getValues().get(1), makeFooUrn(1));
    assertEquals(results.getValues().get(2), makeFooUrn(2));
  }

  @Test
  public void testGetAspectsWithIndexFilter() {
    LocalDAOStorageConfig storageConfig =
        makeLocalDAOStorageConfig(AspectFoo.class, Collections.singletonList("/value"));
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server, storageConfig, FooUrn.class);
    dao.enableLocalSecondaryIndex(true);
    dao.setUrnPathExtractor(new FooUrnPathExtractor());

    FooUrn urn1 = makeFooUrn(1);
    AspectFoo e1foo1 = new AspectFoo().setValue("val1");
    addMetadata(urn1, AspectFoo.class.getCanonicalName(), 0, e1foo1);
    AspectFoo e1foo2 = new AspectFoo().setValue("val2");
    addMetadata(urn1, AspectFoo.class.getCanonicalName(), 1, e1foo2);
    AspectBar e1bar1 = new AspectBar().setValue("val1");
    addMetadata(urn1, AspectBar.class.getCanonicalName(), 0, e1bar1);
    AspectBar e1bar2 = new AspectBar().setValue("val2");
    addMetadata(urn1, AspectBar.class.getCanonicalName(), 1, e1bar2);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo e2foo1 = new AspectFoo().setValue("val1");
    addMetadata(urn2, AspectFoo.class.getCanonicalName(), 0, e2foo1);
    AspectFoo e2foo2 = new AspectFoo().setValue("val2");
    addMetadata(urn2, AspectFoo.class.getCanonicalName(), 1, e2foo2);
    AspectBar e2bar1 = new AspectBar().setValue("val1");
    addMetadata(urn2, AspectBar.class.getCanonicalName(), 0, e2bar1);
    AspectBar e2bar2 = new AspectBar().setValue("val2");
    addMetadata(urn2, AspectBar.class.getCanonicalName(), 1, e2bar2);

    dao.updateLocalIndex(urn1, e1foo1, 0);
    dao.updateLocalIndex(urn1, e1bar1, 0);
    dao.updateLocalIndex(urn2, e2foo1, 0);
    dao.updateLocalIndex(urn2, e2bar1, 0);

    Set<Class<? extends RecordTemplate>> aspectClasses = ImmutableSet.of(AspectFoo.class, AspectBar.class);
    IndexValue indexValue = new IndexValue();
    indexValue.setString("val1");
    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName())
        .setPathParams(new IndexPathParams().setPath("/value").setValue(indexValue));
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray(Collections.singletonList(criterion));
    IndexFilter indexFilter = new IndexFilter().setCriteria(indexCriterionArray);

    ListResult<UrnAspectEntry<FooUrn>> actual = dao.getAspects(aspectClasses, indexFilter, null, 5);

    UrnAspectEntry<FooUrn> entry1 = new UrnAspectEntry<>(urn1, Arrays.asList(e1foo1, e1bar1));
    UrnAspectEntry<FooUrn> entry2 = new UrnAspectEntry<>(urn2, Arrays.asList(e2foo1, e2bar1));

    assertEquals(actual.getValues(), Arrays.asList(entry1, entry2));
    assertEquals(actual.getPageSize(), 5);
    assertEquals(actual.getTotalPageCount(), 1);
    assertEquals(actual.getTotalCount(), 2);
    assertNull(actual.getMetadata());
  }

  @Test
  public void testList() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    List<AspectFoo> foos = new LinkedList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);

      for (int j = 0; j < 10; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        addMetadata(urn, AspectFoo.class.getCanonicalName(), j, foo);
        if (i == 0) {
          foos.add(foo);
        }
      }
    }

    FooUrn urn0 = makeFooUrn(0);

    ListResult<AspectFoo> results = dao.list(AspectFoo.class, urn0, 0, 5);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 5);
    assertEquals(results.getTotalCount(), 10);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), foos.subList(0, 5));

    assertNotNull(results.getMetadata());
    List<Long> expectedVersions = Arrays.asList(0L, 1L, 2L, 3L, 4L);
    List<Urn> expectedUrns = Arrays.asList(makeFooUrn(0), makeFooUrn(1), makeFooUrn(2), makeFooUrn(3), makeFooUrn(4));
    assertVersionMetadata(results.getMetadata(), expectedVersions, expectedUrns, 1234L, new CorpuserUrn("foo"),
        new CorpuserUrn("bar"));

    // List next page
    results = dao.list(AspectFoo.class, urn0, 5, 9);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 10);
    assertEquals(results.getPageSize(), 9);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), foos.subList(5, 10));
    assertNotNull(results.getMetadata());
  }

  private static LocalDAOStorageConfig makeLocalDAOStorageConfig(Class<? extends RecordTemplate> aspectClass,
      List<String> pegasusPaths) {
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectStorageConfigMap =
        new HashMap<>();
    aspectStorageConfigMap.put(aspectClass, getAspectStorageConfig(pegasusPaths));
    LocalDAOStorageConfig storageConfig =
        LocalDAOStorageConfig.builder().aspectStorageConfigMap(aspectStorageConfigMap).build();
    return storageConfig;
  }

  private static LocalDAOStorageConfig makeLocalDAOStorageConfig(Class<? extends RecordTemplate> aspectClass1,
      List<String> pegasusPaths1, Class<? extends RecordTemplate> aspectClass2, List<String> pegasusPaths2) {
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectStorageConfigMap =
        new HashMap<>();
    aspectStorageConfigMap.put(aspectClass1, getAspectStorageConfig(pegasusPaths1));
    aspectStorageConfigMap.put(aspectClass2, getAspectStorageConfig(pegasusPaths2));
    LocalDAOStorageConfig storageConfig =
        LocalDAOStorageConfig.builder().aspectStorageConfigMap(aspectStorageConfigMap).build();
    return storageConfig;
  }

  private static LocalDAOStorageConfig.AspectStorageConfig getAspectStorageConfig(List<String> pegasusPaths) {
    Map<String, LocalDAOStorageConfig.PathStorageConfig> pathStorageConfigMap = new HashMap<>();
    pegasusPaths.forEach(path -> pathStorageConfigMap.put(path,
        LocalDAOStorageConfig.PathStorageConfig.builder().strongConsistentSecondaryIndex(true).build()));
    return LocalDAOStorageConfig.AspectStorageConfig.builder().pathStorageConfigMap(pathStorageConfigMap).build();
  }

  @Test
  void testStrongConsistentIndexPaths() {
    // construct LocalDAOStorageConfig object
    LocalDAOStorageConfig storageConfig =
        makeLocalDAOStorageConfig(AspectFoo.class, Collections.singletonList("/value"));

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server, storageConfig, FooUrn.class);
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectToPaths =
        dao.getStrongConsistentIndexPaths();

    assertNotNull(aspectToPaths);
    Set<Class<? extends RecordTemplate>> setAspects = aspectToPaths.keySet();
    assertEquals(setAspects, new HashSet<>(Arrays.asList(AspectFoo.class)));
    LocalDAOStorageConfig.AspectStorageConfig config = aspectToPaths.get(AspectFoo.class);
    assertTrue(config.getPathStorageConfigMap().get("/value").isStrongConsistentSecondaryIndex());
  }

  @Test
  public void testListAspectsForAllUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);

      for (int j = 0; j < 10; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + i + j);
        addMetadata(urn, AspectFoo.class.getCanonicalName(), j, foo);
      }
    }

    ListResult<AspectFoo> results = dao.list(AspectFoo.class, 0, 0, 2);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 2);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 2);
    assertEquals(results.getTotalPageCount(), 2);

    assertNotNull(results.getMetadata());
    assertVersionMetadata(results.getMetadata(), Arrays.asList(0L), Arrays.asList(makeFooUrn(0)), 1234L,
        new CorpuserUrn("foo"), new CorpuserUrn("bar"));

    // Test list latest aspects
    ListResult<AspectFoo> latestResults = dao.list(AspectFoo.class, 0, 2);
    assertEquals(results, latestResults);

    // List next page
    results = dao.list(AspectFoo.class, 0, 2, 2);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 2);
    assertEquals(results.getTotalPageCount(), 2);
    assertNotNull(results.getMetadata());

    // Test list for a non-zero version
    results = dao.list(AspectFoo.class, 1, 0, 5);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 1);

    assertNotNull(results.getMetadata());
    assertVersionMetadata(results.getMetadata(), Arrays.asList(1L), Arrays.asList(makeUrn(2)), 1234L,
        new CorpuserUrn("foo"), new CorpuserUrn("bar"));
  }

  @Test
  void testNewStringId() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    String id1 = dao.newStringId();
    String id2 = dao.newStringId();

    assertNotNull(id1);
    assertTrue(id1.length() > 0);
    assertNotNull(id2);
    assertTrue(id2.length() > 0);
    assertNotEquals(id1, id2);
  }

  @Test
  void testNewNumericId() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    long id1 = dao.newNumericId("namespace");
    long id2 = dao.newNumericId("namespace");
    long id3 = dao.newNumericId("another namespace");

    assertEquals(id1, 1);
    assertEquals(id2, 2);
    assertEquals(id3, 1);
  }

  @Test
  void testSaveSingleEntryToLocalIndex() {
    EbeanLocalDAO<EntityAspectUnion, BarUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, BarUrn.class);
    BarUrn urn = makeBarUrn(0);

    // Test indexing integer typed value
    long recordId = dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/intFoo", 0);
    EbeanMetadataIndex record = getRecordFromLocalIndex(recordId);
    assertNotNull(record);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/intFoo");
    assertEquals(record.getLongVal().longValue(), 0L);

    // Test indexing long typed value
    recordId = dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/longFoo", 1L);
    record = getRecordFromLocalIndex(recordId);
    assertNotNull(record);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/longFoo");
    assertEquals(record.getLongVal().longValue(), 1L);

    // Test indexing boolean typed value
    recordId = dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/boolFoo", true);
    record = getRecordFromLocalIndex(recordId);
    assertNotNull(record);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/boolFoo");
    assertEquals(record.getStringVal(), "true");

    // Test indexing float typed value
    recordId = dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/floatFoo", 12.34f);
    record = getRecordFromLocalIndex(recordId);
    assertNotNull(record);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/floatFoo");
    assertEquals(record.getDoubleVal(), 12.34);

    // Test indexing double typed value
    recordId = dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/doubleFoo", 23.45);
    record = getRecordFromLocalIndex(recordId);
    assertNotNull(record);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/doubleFoo");
    assertEquals(record.getDoubleVal(), 23.45);

    // Test indexing string typed value
    recordId = dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/stringFoo", "valFoo");
    record = getRecordFromLocalIndex(recordId);
    assertNotNull(record);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/stringFoo");
    assertEquals(record.getStringVal(), "valFoo");
  }

  @Test
  void testExistsInLocalIndex() {
    EbeanLocalDAO<EntityAspectUnion, BarUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, BarUrn.class);
    BarUrn urn = makeBarUrn(0);

    assertFalse(dao.existsInLocalIndex(urn));

    dao.saveSingleRecordToLocalIndex(urn, BarUrn.class.getCanonicalName(), "/barId", 0);
    assertTrue(dao.existsInLocalIndex(urn));
  }

  @Test
  void testUpdateUrnInLocalIndex() {
    // only urn will be updated since storage config has not been provided
    EbeanLocalDAO<EntityAspectUnion, BarUrn> dao1 =
        new EbeanLocalDAO<EntityAspectUnion, BarUrn>(EntityAspectUnion.class, _mockProducer, _server, BarUrn.class);
    dao1.enableLocalSecondaryIndex(true);
    dao1.setUrnPathExtractor(new BarUrnPathExtractor());
    EbeanLocalDAO<EntityAspectUnion, BazUrn> dao2 =
        new EbeanLocalDAO<EntityAspectUnion, BazUrn>(EntityAspectUnion.class, _mockProducer, _server, BazUrn.class);
    dao2.enableLocalSecondaryIndex(true);
    dao2.setUrnPathExtractor(new BazUrnPathExtractor());

    BarUrn barUrn = makeBarUrn(1);
    BazUrn bazUrn = makeBazUrn(2);
    AspectBar aspectBar = new AspectBar().setValue("val1");
    AspectBaz aspectBaz = new AspectBaz().setBoolField(true).setLongField(1234L).setStringField("val2");

    dao1.updateLocalIndex(barUrn, aspectBar, 0);
    dao2.updateLocalIndex(bazUrn, aspectBaz, 0);

    List<EbeanMetadataIndex> barRecords = getAllRecordsFromLocalIndex(barUrn);
    assertEquals(barRecords.size(), 1);
    EbeanMetadataIndex barRecord = barRecords.get(0);
    assertEquals(barRecord.getUrn(), barUrn.toString());
    assertEquals(barRecord.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(barRecord.getPath(), "/barId");
    assertEquals(barRecord.getLongVal().longValue(), 1L);

    List<EbeanMetadataIndex> bazRecords = getAllRecordsFromLocalIndex(bazUrn);
    assertEquals(bazRecords.size(), 1);
    EbeanMetadataIndex bazRecord = bazRecords.get(0);
    assertEquals(bazRecord.getUrn(), bazUrn.toString());
    assertEquals(bazRecord.getAspect(), BazUrn.class.getCanonicalName());
    assertEquals(bazRecord.getPath(), "/bazId");
    assertEquals(bazRecord.getLongVal().longValue(), 2L);

    // Test if new record is inserted with an existing urn
    dao1.updateLocalIndex(barUrn, aspectBar, 1);
    assertEquals(getAllRecordsFromLocalIndex(barUrn).size(), 1);
  }

  @Test(expectedExceptions = NullPointerException.class)
  void testNullAspectStorageConfigMap() {
    // null aspect storage config map should throw an exception
    LocalDAOStorageConfig.builder().aspectStorageConfigMap(null).build();
  }

  @Test
  void testEmptyAspectStorageConfigMap() {
    FooUrn urn = makeFooUrn(1);

    // default storage config constructed, resulting in empty aspect storage config map
    LocalDAOStorageConfig storageConfig = LocalDAOStorageConfig.builder().build();
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server, storageConfig, FooUrn.class,
            new FooUrnPathExtractor());
    dao.enableLocalSecondaryIndex(true);
    AspectFoo aspect = new AspectFoo().setValue("val1");

    // only urn is updated, aspect isn't
    dao.updateLocalIndex(urn, aspect, 0);
    List<EbeanMetadataIndex> fooRecords = getAllRecordsFromLocalIndex(urn);
    assertEquals(fooRecords.size(), 1);
    EbeanMetadataIndex record = fooRecords.get(0);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), FooUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/fooId");
    assertEquals(record.getLongVal().longValue(), 1L);
  }

  @Test
  void testNullPathStorageConfigMap() {
    FooUrn urn = makeFooUrn(2);

    // path storage config map is manually set as null
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectStorageConfigMap =
        new HashMap<>();
    aspectStorageConfigMap.put(AspectFoo.class, null);
    LocalDAOStorageConfig storageConfig =
        LocalDAOStorageConfig.builder().aspectStorageConfigMap(aspectStorageConfigMap).build();
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server, storageConfig, FooUrn.class);
    dao.enableLocalSecondaryIndex(true);
    dao.setUrnPathExtractor(new FooUrnPathExtractor());
    AspectFoo aspect = new AspectFoo().setValue("val2");

    // only urn is updated, aspect isn't
    dao.updateLocalIndex(urn, aspect, 0);
    List<EbeanMetadataIndex> fooRecords = getAllRecordsFromLocalIndex(urn);
    assertEquals(fooRecords.size(), 1);
    EbeanMetadataIndex record = fooRecords.get(0);
    assertEquals(record.getUrn(), urn.toString());
    assertEquals(record.getAspect(), FooUrn.class.getCanonicalName());
    assertEquals(record.getPath(), "/fooId");
    assertEquals(record.getLongVal().longValue(), 2L);
  }

  @Test
  void testUpdateUrnAndAspectInLocalIndex() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server,
        makeLocalDAOStorageConfig(AspectFooEvolved.class, Arrays.asList("/value", "/newValue")), FooUrn.class);
    dao.enableLocalSecondaryIndex(true);
    dao.setUrnPathExtractor(new FooUrnPathExtractor());
    FooUrn urn = makeFooUrn(1);
    AspectFooEvolved aspect1 = new AspectFooEvolved().setValue("val1").setNewValue("newVal1");

    dao.updateLocalIndex(urn, aspect1, 0);
    List<EbeanMetadataIndex> fooRecords1 = getAllRecordsFromLocalIndex(urn);
    assertEquals(fooRecords1.size(), 3);
    EbeanMetadataIndex fooRecord1 = fooRecords1.get(0);
    EbeanMetadataIndex fooRecord2 = fooRecords1.get(1);
    EbeanMetadataIndex fooRecord3 = fooRecords1.get(2);
    assertEquals(fooRecord1.getUrn(), urn.toString());
    assertEquals(fooRecord1.getAspect(), FooUrn.class.getCanonicalName());
    assertEquals(fooRecord1.getPath(), "/fooId");
    assertEquals(fooRecord1.getLongVal().longValue(), 1L);
    assertEquals(fooRecord2.getUrn(), urn.toString());
    assertEquals(fooRecord2.getAspect(), AspectFooEvolved.class.getCanonicalName());
    assertEquals(fooRecord2.getPath(), "/newValue");
    assertEquals(fooRecord2.getStringVal(), "newVal1");
    assertEquals(fooRecord3.getUrn(), urn.toString());
    assertEquals(fooRecord3.getAspect(), AspectFooEvolved.class.getCanonicalName());
    assertEquals(fooRecord3.getPath(), "/value");
    assertEquals(fooRecord3.getStringVal(), "val1");

    // Only the aspect and not urn will be inserted, with an aspect version different than 0. Old aspect rows should be deleted, new inserted
    AspectFooEvolved aspect2 = new AspectFooEvolved().setValue("val2").setNewValue("newVal2");
    dao.updateLocalIndex(urn, aspect2, 1);
    assertEquals(getAllRecordsFromLocalIndex(urn).size(), 3);
    List<EbeanMetadataIndex> fooRecords2 = getAllRecordsFromLocalIndex(urn);
    EbeanMetadataIndex fooRecord4 = fooRecords2.get(0);
    EbeanMetadataIndex fooRecord5 = fooRecords2.get(1);
    EbeanMetadataIndex fooRecord6 = fooRecords2.get(2);
    assertEquals(fooRecord4.getUrn(), urn.toString());
    assertEquals(fooRecord4.getAspect(), FooUrn.class.getCanonicalName());
    assertEquals(fooRecord4.getPath(), "/fooId");
    assertEquals(fooRecord4.getLongVal().longValue(), 1L);
    assertEquals(fooRecord5.getUrn(), urn.toString());
    assertEquals(fooRecord5.getAspect(), AspectFooEvolved.class.getCanonicalName());
    assertEquals(fooRecord5.getPath(), "/newValue");
    assertEquals(fooRecord5.getStringVal(), "newVal2");
    assertEquals(fooRecord6.getUrn(), urn.toString());
    assertEquals(fooRecord6.getAspect(), AspectFooEvolved.class.getCanonicalName());
    assertEquals(fooRecord6.getPath(), "/value");
    assertEquals(fooRecord6.getStringVal(), "val2");

    // if the value of a path is null then the corresponding path should not be inserted. Again old aspect rows should be deleted, new inserted
    AspectFooEvolved aspect3 = new AspectFooEvolved().setValue("val3");
    dao.updateLocalIndex(urn, aspect3, 2);
    assertEquals(getAllRecordsFromLocalIndex(urn).size(), 2);
    List<EbeanMetadataIndex> fooRecords3 = getAllRecordsFromLocalIndex(urn);
    EbeanMetadataIndex fooRecord7 = fooRecords1.get(0);
    EbeanMetadataIndex fooRecord8 = fooRecords3.get(1);
    assertEquals(fooRecord7.getUrn(), urn.toString());
    assertEquals(fooRecord7.getAspect(), FooUrn.class.getCanonicalName());
    assertEquals(fooRecord7.getPath(), "/fooId");
    assertEquals(fooRecord7.getLongVal().longValue(), 1L);
    assertEquals(fooRecord8.getUrn(), urn.toString());
    assertEquals(fooRecord8.getAspect(), AspectFooEvolved.class.getCanonicalName());
    assertEquals(fooRecord8.getPath(), "/value");
    assertEquals(fooRecord8.getStringVal(), "val3");
  }

  @Test
  void testUpdateLocalIndex() {
    EbeanLocalDAO<EntityAspectUnion, BarUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, BarUrn.class);
    dao.enableLocalSecondaryIndex(true);
    dao.setUrnPathExtractor(new BarUrnPathExtractor());

    BarUrn urn = makeBarUrn(1);
    AspectBar aspect = new AspectBar();

    dao.updateLocalIndex(urn, aspect, 0);
    List<EbeanMetadataIndex> barRecords = getAllRecordsFromLocalIndex(urn);
    assertEquals(barRecords.size(), 1);
    EbeanMetadataIndex barRecord = barRecords.get(0);
    assertEquals(barRecord.getUrn(), urn.toString());
    assertEquals(barRecord.getAspect(), BarUrn.class.getCanonicalName());
    assertEquals(barRecord.getPath(), "/barId");
    assertEquals(barRecord.getLongVal().longValue(), 1L);

    // Test if new record is inserted with an aspect version different than 0
    dao.updateLocalIndex(urn, aspect, 1);
    assertEquals(getAllRecordsFromLocalIndex(urn).size(), 1);
  }

  @Test
  void testGetGMAIndexPair() {
    IndexValue indexValue = new IndexValue();
    // 1. IndexValue pair corresponds to boolean
    indexValue.setBoolean(false);
    EbeanLocalDAO.GMAIndexPair gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexValue);
    assertEquals(EbeanMetadataIndex.STRING_COLUMN, gmaIndexPair.valueType);
    assertEquals("false", gmaIndexPair.value);
    // 2. IndexValue pair corresponds to double
    double dVal = 0.000001;
    indexValue.setDouble(dVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexValue);
    assertEquals(EbeanMetadataIndex.DOUBLE_COLUMN, gmaIndexPair.valueType);
    assertEquals(dVal, gmaIndexPair.value);
    // 3. IndexValue pair corresponds to float
    float fVal = 0.0001f;
    double doubleVal = fVal;
    indexValue.setFloat(fVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexValue);
    assertEquals(EbeanMetadataIndex.DOUBLE_COLUMN, gmaIndexPair.valueType);
    assertEquals(doubleVal, gmaIndexPair.value);
    // 4. IndexValue pair corresponds to int
    int iVal = 100;
    long longVal = iVal;
    indexValue.setInt(iVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexValue);
    assertEquals(EbeanMetadataIndex.LONG_COLUMN, gmaIndexPair.valueType);
    assertEquals(longVal, gmaIndexPair.value);
    // 5. IndexValue pair corresponds to long
    long lVal = 1L;
    indexValue.setLong(lVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexValue);
    assertEquals(EbeanMetadataIndex.LONG_COLUMN, gmaIndexPair.valueType);
    assertEquals(lVal, gmaIndexPair.value);
    // 6/ IndexValue pair corresponds to string
    String sVal = "testVal";
    indexValue.setString(sVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexValue);
    assertEquals(EbeanMetadataIndex.STRING_COLUMN, gmaIndexPair.valueType);
    assertEquals(sVal, gmaIndexPair.value);
  }

  @Test
  void testListUrnsFromIndex() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect = "aspect" + System.currentTimeMillis();
    addIndex(urn1, aspect, "/path1", "val1");
    addIndex(urn1, aspect, "/path2", "val2");
    addIndex(urn1, aspect, "/path3", "val3");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);
    addIndex(urn2, aspect, "/path1", "val1");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);
    addIndex(urn3, aspect, "/path1", "val1");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    // 1. local secondary index is not enabled, should throw exception
    IndexCriterion indexCriterion = new IndexCriterion().setAspect(aspect);
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));
    dao.enableLocalSecondaryIndex(false);

    assertThrows(UnsupportedOperationException.class, () -> dao.listUrns(indexFilter1, null, 2));

    // for the remaining tests, enable writes to local secondary index
    dao.enableLocalSecondaryIndex(true);

    // 2. index criterion array is empty, should throw exception
    final IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray());

    assertThrows(UnsupportedOperationException.class, () -> dao.listUrns(indexFilter2, null, 2));

    // 3. index criterion array contains more than 10 criterion, should throw an exception
    final IndexFilter indexFilter3 = new IndexFilter().setCriteria(makeIndexCriterionArray(11));
    assertThrows(UnsupportedOperationException.class, () -> dao.listUrns(indexFilter3, null, 2));

    // 3. only aspect and not path or value is provided in Index Filter
    indexCriterion = new IndexCriterion().setAspect(aspect);
    final IndexFilter indexFilter4 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));

    ListResult<FooUrn> urns = dao.listUrns(indexFilter4, null, 2);

    assertEquals(urns.getValues(), Arrays.asList(urn1, urn2));
    assertEquals(urns.getTotalCount(), 3);

    // 5. aspect with path and value is provided in index filter
    IndexValue indexValue = new IndexValue();
    indexValue.setString("val1");
    IndexPathParams indexPathParams = new IndexPathParams().setPath("/path1").setValue(indexValue);
    indexCriterion = new IndexCriterion().setAspect(aspect).setPathParams(indexPathParams);
    final IndexFilter indexFilter5 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));

    urns = dao.listUrns(indexFilter5, urn1, 2);

    assertEquals(urns.getValues(), Arrays.asList(urn2, urn3));

    // 6. aspect with correct path but incorrect value
    indexValue.setString("valX");
    indexPathParams = new IndexPathParams().setPath("/path1").setValue(indexValue);
    indexCriterion = new IndexCriterion().setAspect(aspect).setPathParams(indexPathParams);
    final IndexFilter indexFilter6 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));

    urns = dao.listUrns(indexFilter6, urn1, 2);

    assertEquals(urns.getTotalCount(), 0);
  }

  @Test
  void testAddEntityTypeFilter() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);

    String aspect = "aspect" + System.currentTimeMillis();
    IndexValue indexValue = new IndexValue();
    indexValue.setString("val1");
    IndexCriterion indexCriterion1 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setValue(indexValue).setPath("path"));
    IndexCriterion indexCriterion2 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());

    IndexFilter filter1 =
        new IndexFilter().setCriteria(new IndexCriterionArray(Arrays.asList(indexCriterion1, indexCriterion2)));
    IndexFilter filter2 =
        new IndexFilter().setCriteria(new IndexCriterionArray(Collections.singletonList(indexCriterion1)));
    IndexFilter filter3 =
        new IndexFilter().setCriteria(new IndexCriterionArray(Arrays.asList(indexCriterion1, indexCriterion2)));

    // entity class is not set in the index filter
    dao.addEntityTypeFilter(filter2);
    assertEquals(filter2, filter1);

    // if entity class already added, then no further changes
    dao.addEntityTypeFilter(filter3);
    assertEquals(filter3, filter1);
  }

  @Test
  void testListUrnsFromIndexForAnEntity() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao1 =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> dao2 =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, BarUrn.class);
    dao1.enableLocalSecondaryIndex(true);
    dao2.enableLocalSecondaryIndex(true);
    dao1.setUrnPathExtractor(new FooUrnPathExtractor());
    dao2.setUrnPathExtractor(new BarUrnPathExtractor());

    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    BarUrn urn4 = makeBarUrn(4);
    AspectFoo aspectFoo = new AspectFoo();
    AspectBar aspectBar = new AspectBar();

    dao1.updateLocalIndex(urn1, aspectFoo, 0);
    dao1.updateLocalIndex(urn2, aspectFoo, 0);
    dao1.updateLocalIndex(urn3, aspectFoo, 0);
    dao2.updateLocalIndex(urn4, aspectBar, 0);

    // List foo urns
    ListResult<FooUrn> urns1 = dao1.listUrns(FooUrn.class, null, 2);
    assertEquals(urns1.getValues(), Arrays.asList(urn1, urn2));
    assertEquals(urns1.getTotalCount(), 3);

    // List bar urns
    ListResult<BarUrn> urns2 = dao2.listUrns(BarUrn.class, null, 1);
    assertEquals(urns2.getValues(), Arrays.asList(urn4));
    assertEquals(urns2.getTotalCount(), 1);
  }

  @Test
  void testGetUrn() {
    // case 1: valid urn
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    String urn1 = "urn:li:entityFoo:1";
    FooUrn fooUrn = makeFooUrn(1);

    assertEquals(fooUrn, dao.getUrn(urn1));

    // case 2: invalid entity type, correct id
    String urn2 = "urn:li:test:1";
    assertThrows(IllegalArgumentException.class, () -> dao.getUrn(urn2));

    // case 3: invalid urn
    String urn3 = "badUrn";
    assertThrows(IllegalArgumentException.class, () -> dao.getUrn(urn3));
  }

  @Test
  public void testGetWithExtraInfoLatestVersion() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    CorpuserUrn creator1 = new CorpuserUrn("testCreator1");
    CorpuserUrn impersonator1 = new CorpuserUrn("testImpersonator1");
    CorpuserUrn creator2 = new CorpuserUrn("testCreator2");
    CorpuserUrn impersonator2 = new CorpuserUrn("testImpersonator2");
    addMetadataWithAuditStamp(urn, AspectFoo.class.getCanonicalName(), 0, v0, 123, creator1.toString(),
        impersonator1.toString());
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectFoo.class.getCanonicalName(), 1, v1, 456, creator2.toString(),
        impersonator2.toString());

    Optional<AspectWithExtraInfo<AspectFoo>> foo = dao.getWithExtraInfo(AspectFoo.class, urn);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), new AspectWithExtraInfo<>(v0,
        new ExtraInfo().setAudit(makeAuditStamp(creator1, impersonator1, 123)).setUrn(urn).setVersion(0)));
  }

  @Test
  public void testGetWithExtraInfoSpecificVersion() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    CorpuserUrn creator1 = new CorpuserUrn("testCreator1");
    CorpuserUrn impersonator1 = new CorpuserUrn("testImpersonator1");
    CorpuserUrn creator2 = new CorpuserUrn("testCreator2");
    CorpuserUrn impersonator2 = new CorpuserUrn("testImpersonator2");
    addMetadataWithAuditStamp(urn, AspectFoo.class.getCanonicalName(), 0, v0, 123, creator1.toString(),
        impersonator1.toString());
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectFoo.class.getCanonicalName(), 1, v1, 456, creator2.toString(),
        impersonator2.toString());

    Optional<AspectWithExtraInfo<AspectFoo>> foo = dao.getWithExtraInfo(AspectFoo.class, urn, 1);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), new AspectWithExtraInfo<>(v1,
        new ExtraInfo().setAudit(makeAuditStamp(creator2, impersonator2, 456)).setVersion(1).setUrn(urn)));
  }

  @Test
  public void testGetWithExtraInfoMultipleKeys() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, _server, FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    CorpuserUrn creator1 = new CorpuserUrn("testCreator1");
    CorpuserUrn impersonator1 = new CorpuserUrn("testImpersonator1");
    CorpuserUrn creator2 = new CorpuserUrn("testCreator2");
    CorpuserUrn impersonator2 = new CorpuserUrn("testImpersonator2");
    CorpuserUrn creator3 = new CorpuserUrn("testCreator3");
    CorpuserUrn impersonator3 = new CorpuserUrn("testImpersonator3");
    AspectFoo fooV0 = new AspectFoo().setValue("foo");
    addMetadataWithAuditStamp(urn, AspectFoo.class.getCanonicalName(), 0, fooV0, 123, creator1.toString(),
        impersonator1.toString());
    AspectFoo fooV1 = new AspectFoo().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectFoo.class.getCanonicalName(), 1, fooV1, 456, creator2.toString(),
        impersonator2.toString());
    AspectBar barV0 = new AspectBar().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectBar.class.getCanonicalName(), 0, barV0, 1234, creator3.toString(),
        impersonator3.toString());

    // both aspect keys exist
    AspectKey<FooUrn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, urn, 1L);
    AspectKey<FooUrn, AspectBar> aspectKey2 = new AspectKey<>(AspectBar.class, urn, 0L);

    Map<AspectKey<FooUrn, ? extends RecordTemplate>, AspectWithExtraInfo<? extends RecordTemplate>> result =
        dao.getWithExtraInfo(new HashSet<>(Arrays.asList(aspectKey1, aspectKey2)));

    assertEquals(result.keySet().size(), 2);
    assertEquals(result.get(aspectKey1), new AspectWithExtraInfo<>(fooV1,
        new ExtraInfo().setAudit(makeAuditStamp(creator2, impersonator2, 456)).setVersion(1).setUrn(urn)));
    assertEquals(result.get(aspectKey2), new AspectWithExtraInfo<>(barV0,
        new ExtraInfo().setAudit(makeAuditStamp(creator3, impersonator3, 1234)).setVersion(0).setUrn(urn)));

    // one of the aspect keys does not exist
    AspectKey<FooUrn, AspectBar> aspectKey3 = new AspectKey<>(AspectBar.class, urn, 1L);

    result = dao.getWithExtraInfo(new HashSet<>(Arrays.asList(aspectKey1, aspectKey3)));

    assertEquals(result.keySet().size(), 1);
    assertEquals(result.get(aspectKey1), new AspectWithExtraInfo<>(fooV1,
        new ExtraInfo().setAudit(makeAuditStamp(creator2, impersonator2, 456)).setVersion(1).setUrn(urn)));
  }

  private void addMetadata(Urn urn, String aspectName, long version, RecordTemplate metadata) {
    EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    aspect.setKey(new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, version));
    aspect.setMetadata(RecordUtils.toJsonString(metadata));
    aspect.setCreatedOn(new Timestamp(1234));
    aspect.setCreatedBy("urn:li:corpuser:foo");
    aspect.setCreatedFor("urn:li:corpuser:bar");
    _server.save(aspect);
  }

  private void addMetadataWithAuditStamp(Urn urn, String aspectName, long version, RecordTemplate metadata,
      long timeStamp, String creator, String impersonator) {
    EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    aspect.setKey(new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, version));
    aspect.setMetadata(RecordUtils.toJsonString(metadata));
    aspect.setCreatedOn(new Timestamp(timeStamp));
    aspect.setCreatedBy(creator);
    aspect.setCreatedFor(impersonator);
    _server.save(aspect);
  }

  private EbeanMetadataIndex getRecordFromLocalIndex(long id) {
    return _server.find(EbeanMetadataIndex.class, id);
  }

  private <URN extends Urn> List<EbeanMetadataIndex> getAllRecordsFromLocalIndex(URN urn) {
    return _server.find(EbeanMetadataIndex.class).where().eq(EbeanMetadataIndex.URN_COLUMN, urn.toString()).findList();
  }

  private void addIndex(Urn urn, String aspectName, String pathName, Object val) {
    EbeanMetadataIndex index = new EbeanMetadataIndex();
    index.setUrn(urn.toString()).setAspect(aspectName).setPath(pathName);
    if (val instanceof String) {
      index.setStringVal(val.toString());
    } else if (val instanceof Boolean) {
      index.setStringVal(String.valueOf(val));
    } else if (val instanceof Double) {
      index.setDoubleVal((Double) val);
    } else if (val instanceof Float) {
      index.setDoubleVal(((Float) val).doubleValue());
    } else if (val instanceof Integer) {
      index.setLongVal(Long.valueOf((Integer) val));
    } else if (val instanceof Long) {
      index.setLongVal((Long) val);
    } else {
      return;
    }
    _server.save(index);
  }

  private EbeanMetadataAspect getMetadata(Urn urn, String aspectName, long version) {
    return _server.find(EbeanMetadataAspect.class,
        new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, version));
  }

  private void assertVersionMetadata(ListResultMetadata listResultMetadata, List<Long> versions, List<Urn> urns,
      Long time, Urn actor, Urn impersonator) {
    List<ExtraInfo> extraInfos = listResultMetadata.getExtraInfos();
    assertEquals(extraInfos.stream().map(ExtraInfo::getVersion).collect(Collectors.toSet()), versions);

    extraInfos.forEach(v -> {
      assertEquals(v.getAudit().getTime(), time);
      assertEquals(v.getAudit().getActor(), actor);
      assertEquals(v.getAudit().getImpersonator(), impersonator);
    });
  }
}
