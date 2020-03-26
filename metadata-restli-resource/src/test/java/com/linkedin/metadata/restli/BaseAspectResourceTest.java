package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.validator.InvalidSchemaException;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.AspectInvalid;
import com.linkedin.testing.EntityAspectUnion;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseAspectResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;
  private Urn _urn;

  class ValidResource extends BaseAspectResource<Urn, EntityAspectUnion, AspectFoo> {

    ValidResource() {
      super(EntityAspectUnion.class, AspectFoo.class);
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

  class InvalidResource extends BaseAspectResource<Urn, EntityAspectUnion, AspectInvalid> {

    InvalidResource() {
      super(EntityAspectUnion.class, AspectInvalid.class);
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
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testInvalidClass() {
    new InvalidResource();
    fail("No exception");
  }

  @Test
  public void testCreate() throws URISyntaxException {
    ValidResource resource = new ValidResource();
    _urn = makeUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");

    UpdateResponse response = runAndWait(resource.update(foo));

    assertEquals(response.getStatus().getCode(), 201);

    ArgumentCaptor<Function> updateLambdaCaptor = ArgumentCaptor.forClass(Function.class);
    verify(_mockLocalDAO, times(1)).add(eq(_urn), eq(AspectFoo.class), updateLambdaCaptor.capture(),
        any(AuditStamp.class));
    assertEquals(updateLambdaCaptor.getValue().apply(Optional.empty()), foo);

    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testGet() throws URISyntaxException {
    ValidResource resource = new ValidResource();
    _urn = makeUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    when(_mockLocalDAO.get(AspectFoo.class, _urn, BaseLocalDAO.LATEST_VERSION)).thenReturn(Optional.of(foo));

    AspectFoo result = runAndWait(resource.get(0));

    assertEquals(result, foo);
  }
}
