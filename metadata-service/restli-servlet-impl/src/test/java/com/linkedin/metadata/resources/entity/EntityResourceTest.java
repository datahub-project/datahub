package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Task;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityResourceTest {
  private EntityResource entityResource;
  private EntityService<?> entityService;
  private TimeseriesAspectService timeseriesAspectService;
  private Authorizer authorizer;
  private OperationContext systemOperationContext;

  @BeforeMethod
  public void setup() {
    entityResource = new EntityResource();
    entityService = mock(EntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
            });
    when(timeseriesAspectService.deleteAspectValues(
            any(OperationContext.class), any(), any(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(0L));
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    entityResource.setEntityService(entityService);
    entityResource.setTimeseriesAspectService(timeseriesAspectService);
    entityResource.setAuthorizer(authorizer);
    entityResource.setSystemOperationContext(systemOperationContext);

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);
  }

  /**
   * Regression for INC-3052: when {@link RollbackRunResult#getRowsDeletedFromEntityDeletion()}
   * returns null (e.g. {@code RollbackResult.additionalRowsAffected} was never set), the previous
   * code unboxed null into the generated primitive-long {@code DeleteEntityResponse.setRows(long)}
   * and threw NullPointerException. Customer-driven mass CLI deletes hit this path repeatedly,
   * triggering High5xxRate. The fix defaults null to 0L.
   */
  @Test
  public void testDeleteEntityHandlesNullRowsDeletedFromEntityDeletion()
      throws URISyntaxException {
    RollbackRunResult resultWithNullRows =
        new RollbackRunResult(Collections.emptyList(), null, Collections.emptyList());
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(resultWithNullRows);

    Urn urn = new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD);

    Task<DeleteEntityResponse> task = entityResource.deleteEntity(urn.toString(), null, null, null);

    assertNotNull(task);
  }
}
