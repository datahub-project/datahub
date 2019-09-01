package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.Aspect;
import com.linkedin.testing.AspectArray;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.Key;
import com.linkedin.testing.Snapshot;
import com.linkedin.testing.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.restli.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseEntityResourceTest extends BaseEngineTest {

  private BaseLocalDAO<Aspect, Urn> _mockLocalDAO;
  private TestResource _resource = new TestResource();

  class TestResource extends BaseEntityResource<Key, Value, Urn, Snapshot, Aspect> {

    public TestResource() {
      super(Snapshot.class, Aspect.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<Aspect, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected Urn toUrn(@Nonnull Key key) {
      return makeUrn(key.getId());
    }

    @Nonnull
    @Override
    protected Key toKey(@Nonnull Urn urn) {
      return new Key().setId(urn.getIdAsLong());
    }

    @Nonnull
    @Override
    protected Value toValue(@Nonnull Snapshot snapshot) {
      Value value = new Value();
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
    protected Snapshot toSnapshot(@Nonnull Value value, @Nonnull Urn urn) {
      Snapshot snapshot = new Snapshot().setUrn(urn);
      AspectArray aspects = new AspectArray();
      if (value.hasFoo()) {
        aspects.add(ModelUtils.newAspectUnion(Aspect.class, value.getFoo()));
      }
      if (value.hasBar()) {
        aspects.add(ModelUtils.newAspectUnion(Aspect.class, value.getBar()));
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
    Urn urn = makeUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, BaseLocalDAO.LATEST_VERSION);

    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    Value value = runAndWait(_resource.get(makeResourceKey(urn), new String[0]));

    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testGetSpecificAspect() {
    Urn urn = makeUrn(1234);
    AspectKey<Urn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, BaseLocalDAO.LATEST_VERSION);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};

    runAndWait(_resource.get(makeResourceKey(urn), aspectNames));

    verify(_mockLocalDAO, times(1)).get(Collections.singleton(aspect1Key));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testBatchGet() {
    Urn urn1 = makeUrn(1);
    Urn urn2 = makeUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<Urn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, BaseLocalDAO.LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooKey1, aspectBarKey1, aspectFooKey2, aspectBarKey2))).thenReturn(
        ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectFooKey2, Optional.of(bar)));

    Map<Key, Value> keyValueMap =
        runAndWait(_resource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), new String[0])).entrySet()
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
    Urn urn1 = makeUrn(1);
    Urn urn2 = makeUrn(2);
    AspectKey<Urn, AspectFoo> fooKey1 = new AspectKey<>(AspectFoo.class, urn1, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectFoo> fooKey2 = new AspectKey<>(AspectFoo.class, urn2, BaseLocalDAO.LATEST_VERSION);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class)};

    runAndWait(_resource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), aspectNames));

    verify(_mockLocalDAO, times(1)).get(ImmutableSet.of(fooKey1, fooKey2));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testUpdate() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    Value value = new Value().setFoo(foo).setBar(bar);

    runAndWait(_resource.update(makeResourceKey(urn), value));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any());
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any());
    verifyNoMoreInteractions(_mockLocalDAO);
  }
}
