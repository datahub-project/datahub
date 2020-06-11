package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.singleaspectentity.EntityAspectUnion;
import com.linkedin.testing.singleaspectentity.EntitySnapshot;
import com.linkedin.testing.singleaspectentity.EntityValue;
import com.linkedin.testing.urn.SingleAspectEntityUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.LATEST_VERSION;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BaseSingleAspectEntitySimpleKeyResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, SingleAspectEntityUrn> _mockLocalDao;
  private TestResource _resource = new TestResource();

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setup() {
    _mockLocalDao = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGet() throws URISyntaxException {
    long id1 = 100L;
    String field1 = "foo";

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectBar aspect = new AspectBar().setValue(field1);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    when(_mockLocalDao.get(Collections.singleton(aspectKey)))
        .thenReturn(Collections.singletonMap(aspectKey, Optional.of(aspect)));

    EntityValue result = runAndWait(_resource.get(id1, new String[0]));

    assertEquals(result.getValue(), field1);
  }

  @Test
  public void testBatchGet() throws URISyntaxException {
    long id1 = 100L;
    String field1 = "foo";
    int field2 = 1000;
    long id2 = 200L;
    String field11 = "bar";
    int field21 = 2000;
    Set<Long> ids = new HashSet<>(Arrays.asList(id1, id2));

    SingleAspectEntityUrn urn1 = new SingleAspectEntityUrn(id1);
    AspectBar aspect1 = new AspectBar().setValue(field1);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);

    SingleAspectEntityUrn urn2 = new SingleAspectEntityUrn(id2);
    AspectBar aspect2 = new AspectBar().setValue(field11);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);

    Set<AspectKey<SingleAspectEntityUrn, ? extends RecordTemplate>> keys = new HashSet<>(Arrays.asList(aspectKey1, aspectKey2));
    Map<AspectKey<SingleAspectEntityUrn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> keyAspectMap = new HashMap<>();
    keyAspectMap.put(aspectKey1, Optional.of(aspect1));
    keyAspectMap.put(aspectKey2, Optional.of(aspect2));

    when(_mockLocalDao.get(keys)).thenReturn(keyAspectMap);

    Map<Long, EntityValue> result = runAndWait(_resource.batchGet(ids, new String[0]));

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey(id1));
    assertTrue(result.containsKey(id2));
    assertEquals(result.get(id1).getValue(), field1);
    assertEquals(result.get(id2).getValue(), field11);
  }

  @Test
  public void testGetNotFound() throws URISyntaxException {
    long id1 = 100L;

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    when(_mockLocalDao.get(Collections.singleton(aspectKey)))
        .thenReturn(Collections.emptyMap());

    try {
      runAndWait(_resource.get(id1, new String[0]));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
    }
  }

  @Test
  public void testIngest() throws URISyntaxException {
    long id1 = 100L;
    String field1 = "foo";

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectBar aspect = new AspectBar()
        .setValue(field1);
    List<EntityAspectUnion> aspectUnions = Collections.singletonList(
        ModelUtils.newAspectUnion(EntityAspectUnion.class, aspect));

    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspectUnions);

    runAndWait(_resource.ingest(snapshot));

    verify(_mockLocalDao, times(1)).add(eq(urn), eq(aspect), any());
    verifyNoMoreInteractions(_mockLocalDao);
  }

  @Test
  public void testGetSnapshot() throws URISyntaxException {
    long id1 = 100L;
    String field1 = "foo";
    int field2 = 1000;

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectBar aspect = new AspectBar()
        .setValue(field1);
    List<EntityAspectUnion> aspectUnions = Collections.singletonList(
        ModelUtils.newAspectUnion(EntityAspectUnion.class, aspect));
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDao.get(Collections.singleton(aspectKey)))
        .thenReturn(Collections.singletonMap(aspectKey, Optional.of(aspect)));

    EntitySnapshot resultSnapshot = runAndWait(_resource.getSnapshot(urn.toString(), new String[0]));
    assertEquals(resultSnapshot, ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspectUnions));
  }

  @Test
  public void testGetSnapshotWithInvalidUrn() throws URISyntaxException {
    long id1 = 100L;
    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDao.get(aspectKey)).thenReturn(Optional.empty());
    try {
      runAndWait(_resource.getSnapshot(urn.toString(), new String[0]));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
      assertEquals(e.getMessage(), String.format("Urn %s not found.", urn));
    }
  }

  @Test
  public void testBackfill() throws URISyntaxException {
    long id1 = 100L;

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);

    runAndWait(_resource.backfill(urn.toString(), new String[0]));
    verify(_mockLocalDao, times(1)).backfill(eq(AspectBar.class), eq(urn));
    verifyNoMoreInteractions(_mockLocalDao);
  }

  @Test
  public void testBackfillWithInvalidUrn() {
    try {
      runAndWait(_resource.backfill("invalid_urn", new String[0]));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
    }
  }

  /**
   * Test implementation of BaseSingleAspectEntitySimpleKeyResource.
   * */
  private class TestResource extends BaseSingleAspectEntitySimpleKeyResource<
          Long, EntityValue, SingleAspectEntityUrn, AspectBar, EntityAspectUnion, EntitySnapshot> {

    TestResource() {
      super(AspectBar.class, EntityAspectUnion.class, EntityValue.class, EntitySnapshot.class);
    }

    @Override
    @Nonnull
    protected BaseLocalDAO<EntityAspectUnion, SingleAspectEntityUrn> getLocalDAO() {
      return _mockLocalDao;
    }

    @Override
    @Nonnull
    protected SingleAspectEntityUrn createUrnFromString(@Nonnull String urnString) throws Exception {
      return SingleAspectEntityUrn.createFromString(urnString);
    }

    @Override
    @Nonnull
    protected SingleAspectEntityUrn toUrn(@Nonnull Long aLong) {
      try {
        return new SingleAspectEntityUrn(aLong);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    @Nonnull
    protected EntityValue createEntity(@Nonnull EntityValue partialEntity, @Nonnull SingleAspectEntityUrn urn) {
      return partialEntity.setId(urn.getIdAsLong());
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }
}
