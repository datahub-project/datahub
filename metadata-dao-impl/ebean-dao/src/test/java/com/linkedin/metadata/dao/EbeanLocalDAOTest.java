package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.equality.AlwaysFalseEqualityTester;
import com.linkedin.metadata.dao.equality.DefaultEqualityTester;
import com.linkedin.metadata.dao.exception.InvalidMetadataType;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.AspectInvalid;
import com.linkedin.testing.EntityAspectUnion;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.Transaction;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.persistence.RollbackException;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.utils.TestUtils.*;
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);

    dao.add(makeUrn(1), new AspectInvalid().setValue("invalid"), _dummyAuditStamp);
  }

  @Test
  public void testAddOne() {
    Clock mockClock = mock(Clock.class);
    when(mockClock.millis()).thenReturn(1234L);
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    dao.setClock(mockClock);
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    dao.setEqualityTester(AspectFoo.class, DefaultEqualityTester.<AspectFoo>newInstance());
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    dao.setEqualityTester(AspectFoo.class, AlwaysFalseEqualityTester.<AspectFoo>newInstance());
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    dao.setRetention(AspectFoo.class, new VersionBasedRetention(2));
    Urn urn = makeUrn(1);
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

    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    dao.setClock(mockClock);
    dao.setRetention(AspectFoo.class, new TimeBasedRetention(100));
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, server);

    dao.add(makeUrn(1), new AspectFoo().setValue("foo"), _dummyAuditStamp);
  }

  @Test(expectedExceptions = RetryLimitReached.class)
  public void testAddFailedAfterRetry() {
    EbeanServer server = mock(EbeanServer.class);
    Transaction mockTransaction = mock(Transaction.class);
    when(server.beginTransaction()).thenReturn(mockTransaction);
    when(server.find(any(), any())).thenReturn(null);
    doThrow(RollbackException.class).when(server).insert(any(EbeanMetadataAspect.class));
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, server);

    dao.add(makeUrn(1), new AspectFoo().setValue("foo"), _dummyAuditStamp);
  }

  @Test
  public void testGetNonExisting() {
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn);

    assertFalse(foo.isPresent());
  }

  @Test
  public void testGetCapsSensitivity() {
    final EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);

    // urn1 has both foo & bar
    Urn urn1 = makeUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    addMetadata(urn1, AspectFoo.class.getCanonicalName(), 0, foo1);
    AspectBar bar1 = new AspectBar().setValue("bar1");
    addMetadata(urn1, AspectBar.class.getCanonicalName(), 0, bar1);

    // urn2 has only foo
    Urn urn2 = makeUrn(2);
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    addMetadata(urn2, AspectFoo.class.getCanonicalName(), 0, foo2);

    // urn3 has nothing
    Urn urn3 = makeUrn(3);

    Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> result =
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);
    AspectFoo expected = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class.getCanonicalName(), 0, expected);

    Optional<AspectFoo> foo = dao.backfill(AspectFoo.class, urn);

    assertEquals(foo.get(), expected);

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, expected, expected);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testListVersions() {
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    Urn urn = makeUrn(1);
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

  @Test
  public void testListUrns() {
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    AspectFoo foo = new AspectFoo().setValue("foo");
    List<Urn> urns = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Urn urn = makeUrn(i);
      for (int j = 0; j < 3; j++) {
        addMetadata(urn, AspectFoo.class.getCanonicalName(), j, foo);
      }
      urns.add(urn);
    }

    ListResult<Urn> results = dao.listUrns(AspectFoo.class, 0, 1);

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
    assertEquals(results.getValues().get(0), makeUrn(0));
    assertEquals(results.getValues().get(1), makeUrn(1));
    assertEquals(results.getValues().get(2), makeUrn(2));
  }

  @Test
  public void testList() {
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    List<AspectFoo> foos = new LinkedList<>();
    for (int i = 0; i < 3; i++) {
      Urn urn = makeUrn(i);

      for (int j = 0; j < 10; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        addMetadata(urn, AspectFoo.class.getCanonicalName(), j, foo);
        if (i == 0) {
          foos.add(foo);
        }
      }
    }

    Urn urn0 = makeUrn(0);

    ListResult<AspectFoo> results = dao.list(AspectFoo.class, urn0, 0, 5);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 5);
    assertEquals(results.getTotalCount(), 10);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), foos.subList(0, 5));

    assertNotNull(results.getMetadata());
    List<Long> expectedVersions = Arrays.asList(0L, 1L, 2L, 3L, 4L);
    List<Urn> expectedUrns = Arrays.asList(makeUrn(0), makeUrn(1), makeUrn(2), makeUrn(3), makeUrn(4));
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

  @Test
  public void testListAspectsForAllUrns() {
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    for (int i = 0; i < 3; i++) {
      Urn urn = makeUrn(i);

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
    assertVersionMetadata(results.getMetadata(), Arrays.asList(0L), Arrays.asList(makeUrn(0)), 1234L,
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
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
    EbeanLocalDAO dao = new EbeanLocalDAO(EntityAspectUnion.class, _mockProducer, _server);
    long id1 = dao.newNumericId("namespace");
    long id2 = dao.newNumericId("namespace");
    long id3 = dao.newNumericId("another namespace");

    assertEquals(id1, 1);
    assertEquals(id2, 2);
    assertEquals(id3, 1);
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
