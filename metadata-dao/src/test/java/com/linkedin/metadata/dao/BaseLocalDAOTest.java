package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseLocalDAOTest {

  static class DummyLocalDAO extends BaseLocalDAO<EntityAspectUnion, FooUrn> {

    private final BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> _getLatestFunction;

    public DummyLocalDAO(BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> getLatestFunction,
        BaseMetadataEventProducer eventProducer) {
      super(EntityAspectUnion.class, eventProducer);
      _getLatestFunction = getLatestFunction;
    }

    @Override
    protected <ASPECT extends RecordTemplate> long saveLatest(FooUrn urn, Class<ASPECT> aspectClass, ASPECT oldEntry,
        AuditStamp oldAuditStamp, ASPECT newEntry, AuditStamp newAuditStamp) {
      return 0;
    }

    @Override
    protected <ASPECT extends RecordTemplate> void saveToLocalSecondaryIndex(@Nonnull FooUrn urn,
        @Nullable ASPECT newValue, long version) {

    }

    @Override
    protected <T> T runInTransactionWithRetry(Supplier<T> block, int maxTransactionRetry) {
      return block.get();
    }

    @Override
    protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(FooUrn urn, Class<ASPECT> aspectClass) {
      return _getLatestFunction.apply(urn, aspectClass);
    }

    @Override
    protected <ASPECT extends RecordTemplate> long getNextVersion(FooUrn urn, Class<ASPECT> aspectClass) {
      return 0;
    }

    @Override
    protected void save(FooUrn urn, RecordTemplate value, AuditStamp auditStamp, long version, boolean insert) {

    }

    @Override
    protected <ASPECT extends RecordTemplate> void applyVersionBasedRetention(Class<ASPECT> aspectClass, FooUrn urn,
        VersionBasedRetention retention, long largestVersion) {

    }

    @Override
    protected <ASPECT extends RecordTemplate> void applyTimeBasedRetention(Class<ASPECT> aspectClass, FooUrn urn,
        TimeBasedRetention retention, long currentTime) {

    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<Long> listVersions(Class<ASPECT> aspectClass, FooUrn urn,
        int start, int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<Urn> listUrns(Class<ASPECT> aspectClass, int start,
        int pageSize) {
      return null;
    }

    @Override
    public ListResult<Urn> listUrns(@Nonnull IndexFilter indexFilter, @Nullable FooUrn lastUrn, int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(Class<ASPECT> aspectClass, FooUrn urn, int start,
        int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(Class<ASPECT> aspectClass, long version, int start,
        int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(Class<ASPECT> aspectClass, int start, int pageSize) {
      return null;
    }

    @Override
    public long newNumericId(String namespace, int maxTransactionRetry) {
      return 0;
    }

    @Override
    public Map<AspectKey<FooUrn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
        Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys) {
      return null;
    }
  }

  private DummyLocalDAO _dummyLocalDAO;
  private AuditStamp _dummyAuditStamp;
  private BaseMetadataEventProducer _mockEventProducer;
  private BiFunction<FooUrn, Class<? extends RecordTemplate>, BaseLocalDAO.AspectEntry> _mockGetLatestFunction;

  @BeforeMethod
  public void setup() {
    _mockGetLatestFunction = mock(BiFunction.class);
    _mockEventProducer = mock(BaseMetadataEventProducer.class);
    _dummyLocalDAO = new DummyLocalDAO(_mockGetLatestFunction, _mockEventProducer);
    _dummyAuditStamp = makeAuditStamp("foo", 1234);
  }

  private <T extends RecordTemplate> BaseLocalDAO.AspectEntry<T> makeAspectEntry(T aspect, AuditStamp auditStamp) {
    return new BaseLocalDAO.AspectEntry(aspect, new ExtraInfo().setAudit(auditStamp));
  }

  private <T extends RecordTemplate> void expectGetLatest(FooUrn urn, Class<T> aspectClass,
      List<BaseLocalDAO.AspectEntry<T>> returnValues) {
    OngoingStubbing<BaseLocalDAO.AspectEntry<T>> ongoing = when(_mockGetLatestFunction.apply(urn, aspectClass));
    for (BaseLocalDAO.AspectEntry<T> value : returnValues) {
      ongoing = ongoing.thenReturn(value);
    }
  }

  @Test
  public void testMAEEmissionAlways() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    expectGetLatest(urn, AspectFoo.class, Arrays.asList(null, makeAspectEntry(foo, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo, foo);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testMAEEmissionOnValueChange() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(false);
    expectGetLatest(urn, AspectFoo.class, Arrays.asList(null, makeAspectEntry(foo1, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo1, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo2, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo1, foo2);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testMAEEmissionNoValueChange() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo");
    AspectFoo foo2 = new AspectFoo().setValue("foo");
    AspectFoo foo3 = new AspectFoo().setValue("foo");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(false);
    expectGetLatest(urn, AspectFoo.class, Arrays.asList(null, makeAspectEntry(foo1, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo1, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo2, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo3, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testAddSamePostUpdateHookTwice() {
    BiConsumer<FooUrn, AspectFoo> hook = (urn, foo) -> {
      // do nothing;
    };

    _dummyLocalDAO.addPostUpdateHook(AspectFoo.class, hook);

    try {
      _dummyLocalDAO.addPostUpdateHook(AspectFoo.class, hook);
    } catch (IllegalArgumentException e) {
      // expected
      return;
    }

    fail("No IllegalArgumentException thrown");
  }

  @Test
  public void testPostUpdateHookInvoked() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    BiConsumer<FooUrn, AspectFoo> hook = mock(BiConsumer.class);

    _dummyLocalDAO.addPostUpdateHook(AspectFoo.class, hook);
    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);

    verify(hook, times(1)).accept(urn, foo);
    verifyNoMoreInteractions(hook);
  }
}
