package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.utils.TestUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseVersionedAspectResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;
  private TestResource _resource = new TestResource();

  private static final Urn ENTITY_URN = makeUrn(1234);

  class TestResource extends BaseVersionedAspectResource<Urn, EntityAspectUnion, AspectFoo> {

    public TestResource() {
      super(EntityAspectUnion.class, AspectFoo.class);
    }

    @Override
    protected BaseLocalDAO<EntityAspectUnion, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Override
    protected Urn getUrn(@Nonnull PathKeys entityPathKeys) {
      return ENTITY_URN;
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
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, ENTITY_URN, 123L);

    when(_mockLocalDAO.get(aspectKey)).thenReturn(Optional.of(foo));

    AspectFoo result = runAndWait(_resource.get(123L));

    assertEquals(result, foo);
  }

  @Test
  public void testGetAllWithMetadata() {
    List<AspectFoo> foos = ImmutableList.of(new AspectFoo().setValue("v1"), new AspectFoo().setValue("v2"));
    ExtraInfo extraInfo1 = makeExtraInfo(ENTITY_URN, 1L, makeAuditStamp("bar"));
    ExtraInfo extraInfo2 = makeExtraInfo(ENTITY_URN, 2L, makeAuditStamp("baz"));
    ListResultMetadata listResultMetadata =
        new ListResultMetadata().setExtraInfos(new ExtraInfoArray(ImmutableList.of(extraInfo1, extraInfo2)));
    ListResult listResult = ListResult.<AspectFoo>builder().values(foos).metadata(listResultMetadata).build();
    when(_mockLocalDAO.list(AspectFoo.class, ENTITY_URN, 1, 2)).thenReturn(listResult);

    CollectionResult<AspectFoo, ListResultMetadata> collectionResult =
        runAndWait(_resource.getAllWithMetadata(new PagingContext(1, 2)));

    assertEquals(collectionResult.getElements(), foos);
    assertEquals(collectionResult.getMetadata(), listResultMetadata);
  }

  private ExtraInfo makeExtraInfo(Urn urn, Long version, AuditStamp audit) {
    return new ExtraInfo().setUrn(urn).setVersion(version).setAudit(audit);
  }

  @Test
  public void testCreate() {
    AspectFoo foo = new AspectFoo().setValue("foo");

    runAndWait(_resource.create(foo));

    verify(_mockLocalDAO, times(1)).add(eq(ENTITY_URN), eq(foo), any(AuditStamp.class));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testCreateViaLambda() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    Function<Optional<AspectFoo>, AspectFoo> createLambda = (prev) -> foo;

    runAndWait(_resource.create(AspectFoo.class, createLambda));

    verify(_mockLocalDAO, times(1)).add(eq(ENTITY_URN), eq(AspectFoo.class), eq(createLambda), any(AuditStamp.class));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testCreateResponseViaLambda() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    Function<Optional<AspectFoo>, AspectFoo> createLambda = (prev) -> foo;
    when(_mockLocalDAO.add(eq(ENTITY_URN), eq(AspectFoo.class), eq(createLambda), any())).thenReturn(foo);

    CreateKVResponse<Long, AspectFoo> response = runAndWait(_resource.createAndGet(AspectFoo.class, createLambda));

    assertEquals(response.getStatus().getCode(), 201);
    assertEquals(response.getEntity(), foo);
    assertEquals(response.getId(), Long.valueOf(LATEST_VERSION));
  }

  @Test
  public void testCreateIfAbsentWithoutExistingValue() {
    AspectFoo defaultValue = new AspectFoo().setValue("foo");
    when(_mockLocalDAO.add(eq(ENTITY_URN), eq(AspectFoo.class), any(), any())).thenAnswer((InvocationOnMock invocation) -> {
        Object[] args = invocation.getArguments();
        assertTrue(args[2] instanceof Function);
        Function<Optional<RecordTemplate>, RecordTemplate> lambda = (Function<Optional<RecordTemplate>, RecordTemplate>) args[2];
        return lambda.apply(Optional.empty());
    });

    CreateKVResponse<Long, AspectFoo> response = runAndWait(_resource.createIfAbsent(defaultValue));

    assertEquals(response.getStatus().getCode(), 201);
    assertEquals(response.getEntity(), defaultValue);
    assertEquals(response.getId(), Long.valueOf(LATEST_VERSION));
  }

  @Test
  public void testCreateIfAbsentWithExistingValue() {
    AspectFoo oldVal = new AspectFoo().setValue("foo");
    AspectFoo defaultValue = new AspectFoo().setValue("defaultFoo");
    when(_mockLocalDAO.add(eq(ENTITY_URN), eq(AspectFoo.class), any(), any())).thenAnswer((InvocationOnMock invocation) -> {
      Object[] args = invocation.getArguments();
      assertTrue(args[2] instanceof Function);
      Function<Optional<RecordTemplate>, RecordTemplate> lambda = (Function<Optional<RecordTemplate>, RecordTemplate>) args[2];
      return lambda.apply(Optional.of(oldVal));
    });

    CreateKVResponse<Long, AspectFoo> response = runAndWait(_resource.createIfAbsent(defaultValue));

    assertEquals(response.getStatus().getCode(), 201);
    assertEquals(response.getEntity(), oldVal);
    assertEquals(response.getId(), Long.valueOf(LATEST_VERSION));
  }
}
