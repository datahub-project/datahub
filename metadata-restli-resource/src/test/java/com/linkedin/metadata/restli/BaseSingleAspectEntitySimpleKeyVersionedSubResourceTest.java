package com.linkedin.metadata.restli;

import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.internal.server.PathKeysImpl;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.singleaspectentity.EntityAspectUnion;
import com.linkedin.testing.singleaspectentity.EntitySnapshot;
import com.linkedin.testing.singleaspectentity.EntityValue;
import com.linkedin.testing.urn.SingleAspectEntityUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseSingleAspectEntitySimpleKeyVersionedSubResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, SingleAspectEntityUrn> _mockLocalDao;
  private TestVersionedSubResource _resource = new TestVersionedSubResource();

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setup() {
    _mockLocalDao = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGet() throws URISyntaxException {
    long id1 = 100L;
    String field1 = "foo";
    int field2 = 1000;
    long version = 2L;

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectBar aspect = new AspectBar().setValue(field1);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, version);

    when(_mockLocalDao.get(aspectKey)).thenReturn(Optional.of(aspect));

    EntityValue value = runAndWait(_resource.get(version));

    assertEquals(value.getId().longValue(), id1);
    assertEquals(value.getValue(), field1);
  }

  @Test
  public void testGetWithWrongUrnOrVersion() throws URISyntaxException {
    long id1 = 100L;
    long version = 10_000L;

    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id1);
    AspectKey<SingleAspectEntityUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, version);

    when(_mockLocalDao.get(aspectKey)).thenReturn(Optional.empty());

    try {
      runAndWait(_resource.get(version));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
      assertEquals(
          e.getMessage(),
          String.format("Versioned resource for urn %s with the version %s version cannot be found", urn, version));
    }
  }

  @Test
  public void testGetAllWithMetadata() throws URISyntaxException {
    long id = 100L;
    SingleAspectEntityUrn urn = new SingleAspectEntityUrn(id);

    String field1 = "foo";
    AspectBar aspect1 = new AspectBar().setValue(field1);
    EntityValue value1 = new EntityValue().setValue(field1).setId(id);

    String field11 = "bar";
    AspectBar aspect11 = new AspectBar().setValue(field11);
    EntityValue value11 = new EntityValue().setValue(field11).setId(id);

    PagingContext pagingContext1 = new PagingContext(0, 2);
    ListResult<AspectBar> result1 = ListResult.<AspectBar>builder()
        .values(Arrays.asList(aspect1, aspect11))
        .build();

    when(_mockLocalDao.list(AspectBar.class, urn, pagingContext1.getStart(), pagingContext1.getCount()))
        .thenReturn(result1);

    List<EntityValue> elements1 = runAndWait(_resource.getAllWithMetadata(pagingContext1)).getElements();
    assertEquals(elements1, Arrays.asList(value1, value11));
  }

  /**
   * Test sub resource class for BaseSingleAspectEntitySimpleKeyVersionedSubResource.
   * */
  @RestLiCollection(name = "versions", namespace = "com.linkedin.testing", parent = TestResource.class, keyName = "versionId")
  private class TestVersionedSubResource extends BaseSingleAspectEntitySimpleKeyVersionedSubResource<
            EntityValue, SingleAspectEntityUrn, AspectBar, EntityAspectUnion> {

    TestVersionedSubResource() {
      super(AspectBar.class, EntityValue.class);
    }

    @Override
    @Nonnull
    protected BaseLocalDAO<EntityAspectUnion, SingleAspectEntityUrn> getLocalDAO() {
      return _mockLocalDao;
    }

    @Override
    @Nonnull
    protected SingleAspectEntityUrn getUrn(@Nonnull PathKeys keys) {
      try {
        final String keyName = TestResource.class.getAnnotation(RestLiCollection.class).keyName();
        return new SingleAspectEntityUrn(keys.<Long>get(keyName));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    @Nonnull
    protected EntityValue createEntity(@Nonnull EntityValue partialEntity, @Nonnull SingleAspectEntityUrn urn) {
      return partialEntity.setId(urn.getIdAsInt());
    }

    @Override
    public ResourceContext getContext() {
      ResourceContext mock = mock(ResourceContext.class);
      PathKeysImpl keys = new PathKeysImpl();
      keys.append("testId", 100L);
      when(mock.getPathKeys()).thenReturn(keys);
      return mock;
    }
  }

  /**
   * Test resource class for BaseSingleAspectEntitySimpleKeyResource.
   * */
  @RestLiCollection(name = "SingleAspectEntity", namespace = "com.linkedin.testing", keyName = "testId")
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
