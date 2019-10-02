package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.aspect.AspectVersionArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntitySnapshot;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static com.linkedin.metadata.utils.TestUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseSnapshotResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;
  private Urn _urn;
  private Clock _mockClock;

  class SnapshotResource extends BaseSnapshotResource<Urn, EntitySnapshot, EntityAspectUnion> {

    public SnapshotResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, new DummyRestliAuditor(_mockClock));
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<EntityAspectUnion, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected Urn getUrn(@Nonnull PathKeys entityPathKeys) {
      return _urn;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
    _mockClock = mock(Clock.class);
  }

  @Test
  public void testCreate() {
    SnapshotResource resource = new SnapshotResource();
    _urn = makeUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    EntityAspectUnion aspect = ModelUtils.newAspectUnion(EntityAspectUnion.class, foo);
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, _urn, Arrays.asList(aspect));
    when(_mockClock.millis()).thenReturn(100L);
    AuditStamp auditStamp = makeAuditStamp(BaseRestliAuditor.DEFAULT_ACTOR, null, 100);

    CreateResponse response = runAndWait(resource.create(snapshot));

    assertFalse(response.hasError());

    verify(_mockLocalDAO, times(1)).add(_urn, foo, auditStamp);
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testGet() {
    SnapshotResource resource = new SnapshotResource();
    _urn = makeUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, _urn, BaseLocalDAO.LATEST_VERSION);
    when(_mockLocalDAO.get(Collections.singleton(aspectKey))).thenReturn(
        Collections.singletonMap(aspectKey, Optional.of(foo)));
    List<AspectVersion> aspectVersions =
        Lists.newArrayList(ModelUtils.newAspectVersion(AspectFoo.class, BaseLocalDAO.LATEST_VERSION));

    EntitySnapshot snapshot = runAndWait(resource.get(newResourceKey(aspectVersions)));

    assertEquals(snapshot.getAspects().size(), 1);
    assertEquals(snapshot.getAspects().get(0).getAspectFoo(), foo);
  }

  @Test
  public void testBackfill() {
    SnapshotResource resource = new SnapshotResource();
    _urn = makeUrn(1234);
    runAndWait(resource.backfill(new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()}));

    verify(_mockLocalDAO, times(1)).backfill(AspectFoo.class, _urn);
    verify(_mockLocalDAO, times(1)).backfill(AspectBar.class, _urn);
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  private ComplexResourceKey<SnapshotKey, EmptyRecord> newResourceKey(List<AspectVersion> aspectVersions) {
    SnapshotKey snapshotKey = new SnapshotKey();
    snapshotKey.setAspectVersions(new AspectVersionArray(aspectVersions));
    return new ComplexResourceKey<>(snapshotKey, new EmptyRecord());
  }
}
