package io.datahubproject.iceberg.catalog.rest.secure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.net.HttpHeaders;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.client.CacheEvictionService;
import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.lang.reflect.Field;
import java.util.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractControllerTest<T extends AbstractIcebergController> {
  protected static final String TEST_PLATFORM = "test-platform";
  protected static final String TEST_USER = "test-user";
  protected static final String TEST_CREDENTIALS = "test-credentials";

  protected static final String TEST_NAMESPACE = "test_namespace";
  protected static final String TEST_TABLE = "test_table";
  protected static final String TEST_METADATA_LOCATION =
      "s3://test-location/test_table/sample.metadata.json";

  @Mock protected EntityService entityService;
  @Mock protected CredentialProvider credentialProvider;
  @Mock protected Authorizer authorizer;
  @Mock protected HttpServletRequest request;
  @Mock protected SecretService secretService;
  @Mock protected EntitySearchService entitySearchService;
  @Mock protected CacheEvictionService cacheEvictionService;

  private OperationContext systemOperationContext;
  private Authentication authentication;
  private Actor actor;
  protected DataHubIcebergWarehouse warehouse;

  @Mock private RecordTemplate warehouseAspect;
  protected T controller;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(request.getHeader(HttpHeaders.X_FORWARDED_FOR)).thenReturn("1.2.3.4");
    warehouse = mock(DataHubIcebergWarehouse.class);
    when(warehouse.getPlatformInstance()).thenReturn(TEST_PLATFORM);
    when(warehouse.getDataRoot()).thenReturn("s3://someRootLocation");
    setupAuthentication();
    setupController();
    onSetup();
  }

  private void setupAuthentication() {
    actor = new Actor(ActorType.USER, TEST_USER);
    authentication = new Authentication(actor, TEST_CREDENTIALS, Collections.emptyMap());
    AuthenticationContext.setAuthentication(authentication);
  }

  private void setupController() throws Exception {
    controller = newController();
    systemOperationContext =
        TestOperationContexts.systemContext(null, null, null, null, null, null, null, null);

    // Inject dependencies
    injectControllerDependencies();
    setupDefaultAuthorization(true);
  }

  private void injectControllerDependencies() throws Exception {
    injectField("entityService", entityService);
    injectField("secretService", secretService);
    injectField("authorizer", authorizer);
    injectField("systemOperationContext", systemOperationContext);
    injectField("cachingCredentialProvider", credentialProvider);
    injectField("cacheEvictionService", cacheEvictionService);
  }

  protected void setupDefaultAuthorization(boolean isAuthorized) {
    AuthorizationResult.Type resultType =
        isAuthorized ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY;
    String message = isAuthorized ? "Authorized" : "Not authorized";

    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(mock(AuthorizationRequest.class), resultType, message));
  }

  private void injectField(String fieldName, Object value) throws Exception {
    Field field = AbstractIcebergController.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(controller, value);
  }

  private IcebergWarehouseInfo createTestWarehouse() {
    IcebergWarehouseInfo warehouse = new IcebergWarehouseInfo();
    warehouse.setClientId(UrnUtils.getUrn("urn:li:secret:clientId"));
    warehouse.setClientSecret(UrnUtils.getUrn("urn:li:secret:clientSecret"));
    warehouse.setDataRoot("s3://data-root/test/");
    warehouse.setRegion("us-east-1");
    warehouse.setRole("testRole");
    return warehouse;
  }

  protected void onSetup() {}

  protected abstract T newController();
}
