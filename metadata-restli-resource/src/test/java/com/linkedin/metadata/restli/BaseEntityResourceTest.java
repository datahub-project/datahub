package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.UrnAspectEntry;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.EntityKey;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseEntityResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, FooUrn> _mockLocalDAO;
  private TestResource _resource = new TestResource();

  class TestResource extends BaseEntityResource<EntityKey, EntityValue, FooUrn, EntitySnapshot, EntityAspectUnion> {

    public TestResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, FooUrn.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<EntityAspectUnion, FooUrn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected FooUrn createUrnFromString(@Nonnull String urnString) {
      try {
        return FooUrn.createFromString(urnString);
      } catch (URISyntaxException e) {
        throw RestliUtils.badRequestException("Invalid URN: " + urnString);
      }
    }

    @Nonnull
    @Override
    protected FooUrn toUrn(@Nonnull EntityKey key) {
      return makeFooUrn(key.getId().intValue());
    }

    @Nonnull
    @Override
    protected EntityKey toKey(@Nonnull FooUrn urn) {
      return new EntityKey().setId(urn.getIdAsLong());
    }

    @Nonnull
    @Override
    protected EntityValue toValue(@Nonnull EntitySnapshot snapshot) {
      EntityValue value = new EntityValue();
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(a -> {
        if (a instanceof AspectFoo) {
          value.setFoo(AspectFoo.class.cast(a));
        } else if (a instanceof AspectBar) {
          value.setBar(AspectBar.class.cast(a));
        }
      });
      return value;
    }

    @Nonnull
    @Override
    protected EntitySnapshot toSnapshot(@Nonnull EntityValue value, @Nonnull FooUrn urn) {
      EntitySnapshot snapshot = new EntitySnapshot().setUrn(urn);
      EntityAspectUnionArray aspects = new EntityAspectUnionArray();
      if (value.hasFoo()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getFoo()));
      }
      if (value.hasBar()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getBar()));
      }

      snapshot.setAspects(aspects);
      return snapshot;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGet() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), null));

    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testGetNotFound() {
    FooUrn urn = makeFooUrn(1234);

    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key)))).thenReturn(Collections.emptyMap());

    try {
      runAndWait(_resource.get(makeResourceKey(urn), new String[0]));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
      return;
    }

    fail("No exception thrown");
  }

  @Test
  public void testGetSpecificAspect() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};

    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), aspectNames));
    assertEquals(value.getFoo(), foo);
    verify(_mockLocalDAO, times(1)).get(Collections.singleton(aspect1Key));
  }

  @Test
  public void testGetSpecificAspectNotFound() {
    FooUrn urn = makeFooUrn(1234);
    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};
    try {
      runAndWait(_resource.get(makeResourceKey(urn), aspectNames));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
      verify(_mockLocalDAO, times(1)).get(Collections.singleton(aspect1Key));
      verifyNoMoreInteractions(_mockLocalDAO);
      return;
    }
    fail("No exception thrown");
  }

  @Test
  public void testBatchGet() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooKey1, aspectBarKey1, aspectFooKey2, aspectBarKey2))).thenReturn(
        ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectFooKey2, Optional.of(bar)));

    Map<EntityKey, EntityValue> keyValueMap =
        runAndWait(_resource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), null)).entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().getKey(), e -> e.getValue()));

    assertEquals(keyValueMap.size(), 2);
    assertEquals(keyValueMap.get(makeKey(1)).getFoo(), foo);
    assertFalse(keyValueMap.get(makeKey(1)).hasBar());
    assertEquals(keyValueMap.get(makeKey(2)).getBar(), bar);
    assertFalse(keyValueMap.get(makeKey(2)).hasFoo());
  }

  @Test
  public void testBatchGetSpecificAspect() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectKey<FooUrn, AspectFoo> fooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> fooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class)};

    runAndWait(_resource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), aspectNames));

    verify(_mockLocalDAO, times(1)).get(ImmutableSet.of(fooKey1, fooKey2));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testIngest() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any());
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any());
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testSkipIngestAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingestInternal(snapshot, Collections.singleton(AspectBar.class)));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any());
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testGetSnapshotWithOneAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(fooKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(fooKey, Optional.of(foo)));
    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), aspectNames));

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(snapshot.getAspects().size(), 1);
    assertEquals(snapshot.getAspects().get(0).getAspectFoo(), foo);
  }

  @Test
  public void testGetSnapshotWithAllAspects() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");
    AspectKey<FooUrn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(fooKey, barKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(fooKey, Optional.of(foo), barKey, Optional.of(bar)));

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), null));

    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(foo, bar));
  }

  @Test
  public void testGetSnapshotWithInvalidUrn() {
    try {
      runAndWait(_resource.getSnapshot("invalid urn", new String[]{ModelUtils.getAspectName(AspectFoo.class)}));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
      return;
    }

    fail("No exception thrown");
  }

  @Test
  public void testBackfillOneAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    when(_mockLocalDAO.backfill(AspectFoo.class, urn)).thenReturn(Optional.of(foo));
    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};

    BackfillResult backfillResult = runAndWait(_resource.backfill(urn.toString(), aspectNames));

    assertEquals(backfillResult.getEntities().size(), 1);

    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertEquals(backfillResultEntity.getAspects().get(0), ModelUtils.getAspectName(AspectFoo.class));
  }

  @Test
  public void testBackfillAllAspects() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    when(_mockLocalDAO.backfill(AspectFoo.class, urn)).thenReturn(Optional.of(foo));
    when(_mockLocalDAO.backfill(AspectBar.class, urn)).thenReturn(Optional.of(bar));

    BackfillResult backfillResult = runAndWait(_resource.backfill(urn.toString(), null));

    assertEquals(backfillResult.getEntities().size(), 1);

    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertEquals(ImmutableSet.copyOf(backfillResultEntity.getAspects()),
        ImmutableSet.of(ModelUtils.getAspectName(AspectFoo.class), ModelUtils.getAspectName(AspectBar.class)));
  }

  @Test
  public void testBackfillWithInvalidUrn() {
    try {
      runAndWait(_resource.backfill("invalid urn", new String[]{ModelUtils.getAspectName(AspectFoo.class)}));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
      return;
    }

    fail("No exception thrown");
  }

  @Test
  public void testBatchBackfill() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(_mockLocalDAO.backfill(_resource.parseAspectsParam(aspects), ImmutableSet.of(urn1, urn2))).thenReturn(
        ImmutableMap.of(urn1, ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)),
            urn2, ImmutableMap.of(AspectBar.class, Optional.of(bar2))));

    BackfillResult backfillResult =
        runAndWait(_resource.backfill(new String[]{urn1.toString(), urn2.toString()}, aspects));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testBackfillUsingSCSI() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(
        _mockLocalDAO.backfill(BackfillMode.BACKFILL_ALL, _resource.parseAspectsParam(aspects), FooUrn.class, null, 10))
        .thenReturn(ImmutableMap.of(urn1,
            ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)), urn2,
            ImmutableMap.of(AspectBar.class, Optional.of(bar2))));

    BackfillResult backfillResult = runAndWait(_resource.backfill(BackfillMode.BACKFILL_ALL, aspects, null, 10));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testListUrnsFromIndex() {
    // case 1: indexFilter is non-null
    IndexCriterion indexCriterion1 = new IndexCriterion().setAspect("aspect1");
    IndexFilter indexFilter1 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion1));
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    List<FooUrn> urns1 = Arrays.asList(urn2, urn3);
    ListResult<FooUrn> listResult1 = ListResult.<FooUrn>builder().values(urns1).totalCount(100).build();

    when(_mockLocalDAO.listUrns(indexFilter1, urn1, 2)).thenReturn(listResult1);
    String[] actual = runAndWait(_resource.listUrnsFromIndex(indexFilter1, urn1.toString(), 2));
    assertEquals(actual, new String[]{urn2.toString(), urn3.toString()});

    // case 2: indexFilter is null
    IndexCriterion indexCriterion2 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());
    IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion2));
    when(_mockLocalDAO.listUrns(indexFilter2, urn1, 2)).thenReturn(listResult1);
    actual = runAndWait(_resource.listUrnsFromIndex(null, urn1.toString(), 2));
    assertEquals(actual, new String[]{urn2.toString(), urn3.toString()});

    // case 3: lastUrn is null
    List<FooUrn> urns3 = Arrays.asList(urn1, urn2);
    ListResult<FooUrn> listResult3 = ListResult.<FooUrn>builder().values(urns3).totalCount(100).build();
    when(_mockLocalDAO.listUrns(indexFilter2, null, 2)).thenReturn(listResult3);
    actual = runAndWait(_resource.listUrnsFromIndex(null, null, 2));
    assertEquals(actual, new String[]{urn1.toString(), urn2.toString()});
  }

  @Test
  public void testFilterFromIndexEmptyAspects() {
    // case 1: indexFilter is non-null
    IndexCriterion indexCriterion1 = new IndexCriterion().setAspect("aspect1");
    IndexFilter indexFilter1 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion1));
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);

    List<FooUrn> urns1 = Arrays.asList(urn2, urn3);
    ListResult<FooUrn> listResult1 = ListResult.<FooUrn>builder().values(urns1).totalCount(100).build();

    when(_mockLocalDAO.listUrns(indexFilter1, urn1, 2)).thenReturn(listResult1);
    CollectionResult<EntityValue, ListResultMetadata> actual =
        runAndWait(_resource.filter(indexFilter1, new String[0], urn1.toString(), new PagingContext(1, 2)));

    assertEquals(actual.getElements().size(), 2);
    assertEquals(actual.getElements().get(0), new EntityValue());
    assertEquals(actual.getElements().get(1), new EntityValue());
    assertEquals(actual.getTotal().intValue(), 100);
    assertEquals(actual.getMetadata().getExtraInfos().stream().map(ExtraInfo::getUrn).collect(Collectors.toList()),
        Arrays.asList(urn2, urn3));

    // case 2: lastUrn is null
    List<FooUrn> urns2 = Arrays.asList(urn1, urn2);
    IndexCriterion indexCriterion2 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());
    IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion2));
    ListResult<FooUrn> listResult2 = ListResult.<FooUrn>builder().values(urns2).totalCount(500).build();
    when(_mockLocalDAO.listUrns(indexFilter2, null, 2)).thenReturn(listResult2);
    actual = runAndWait(_resource.filter(null, new String[0], null, new PagingContext(0, 2)));
    assertEquals(actual.getElements().size(), 2);
    assertEquals(actual.getElements().get(0), new EntityValue());
    assertEquals(actual.getElements().get(1), new EntityValue());
    assertEquals(actual.getTotal().intValue(), 500);
    assertEquals(actual.getMetadata().getExtraInfos().stream().map(ExtraInfo::getUrn).collect(Collectors.toList()),
        Arrays.asList(urn1, urn2));
  }

  @Test
  public void testFilterFromIndexWithAspects() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("val1");
    AspectFoo foo2 = new AspectFoo().setValue("val2");
    AspectBar bar1 = new AspectBar().setValue("val1");
    AspectBar bar2 = new AspectBar().setValue("val2");

    UrnAspectEntry<FooUrn> entry1 = new UrnAspectEntry<>(urn1, Arrays.asList(foo1, bar1));
    UrnAspectEntry<FooUrn> entry2 = new UrnAspectEntry<>(urn2, Arrays.asList(foo2, bar2));

    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    IndexCriterionArray criterionArray = new IndexCriterionArray(criterion);
    IndexFilter indexFilter = new IndexFilter().setCriteria(criterionArray);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class), ModelUtils.getAspectName(AspectBar.class)};

    // case 1: aspect list is provided, null last urn
    ListResult<UrnAspectEntry<FooUrn>> listResult1 =
        ListResult.<UrnAspectEntry<FooUrn>>builder().values(
            Arrays.asList(entry1, entry2)).totalCount(5).build();

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, null, 2)).thenReturn(
        listResult1);

    CollectionResult<EntityValue, ListResultMetadata> actual1 =
        runAndWait(_resource.filter(indexFilter, aspectNames, null, new PagingContext(0, 2)));

    assertEquals(actual1.getElements().size(), 2);
    assertEquals(actual1.getElements().get(0), new EntityValue().setFoo(foo1).setBar(bar1));
    assertEquals(actual1.getElements().get(1), new EntityValue().setFoo(foo2).setBar(bar2));
    assertEquals(actual1.getTotal().intValue(), 5);
    assertEquals(actual1.getMetadata().getExtraInfos().stream().map(ExtraInfo::getUrn).collect(Collectors.toList()),
        Arrays.asList(urn1, urn2));

    // case 2: null aspects is provided i.e. all aspects in the aspect union will be returned, non-null last urn
    ListResult<UrnAspectEntry<FooUrn>> listResult2 =
        ListResult.<UrnAspectEntry<FooUrn>>builder().values(Collections.singletonList(entry2))
            .totalCount(10)
            .build();

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, urn1, 2)).thenReturn(
        listResult2);

    CollectionResult<EntityValue, ListResultMetadata> actual2 =
        runAndWait(_resource.filter(indexFilter, null, urn1.toString(), new PagingContext(0, 2)));
    assertEquals(actual2.getElements().size(), 1);
    assertEquals(actual2.getElements().get(0), new EntityValue().setFoo(foo2).setBar(bar2));
    assertEquals(actual2.getTotal().intValue(), 10);
    assertEquals(actual2.getMetadata().getExtraInfos().stream().map(ExtraInfo::getUrn).collect(Collectors.toList()),
        Collections.singleton(urn2));
  }

  @Test
  public void testParseAspectsParam() {
    // Only 1 aspect
    Set<Class<? extends RecordTemplate>> aspectClasses =
        _resource.parseAspectsParam(new String[]{AspectFoo.class.getCanonicalName()});
    assertEquals(aspectClasses.size(), 1);
    assertTrue(aspectClasses.contains(AspectFoo.class));

    // No aspect
    aspectClasses = _resource.parseAspectsParam(new String[]{});
    assertEquals(aspectClasses.size(), 0);

    // All aspects
    aspectClasses = _resource.parseAspectsParam(null);
    assertEquals(aspectClasses.size(), 2);
    assertTrue(aspectClasses.contains(AspectFoo.class));
    assertTrue(aspectClasses.contains(AspectBar.class));
  }
}
