package io.datahubproject.iceberg.catalog.rest.secure;

import static com.linkedin.metadata.Constants.CONTAINER_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import io.datahubproject.iceberg.catalog.Utils;
import io.datahubproject.metadata.context.OperationContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.*;
import org.testng.annotations.*;

public class IcebergNamespaceApiControllerTest
    extends AbstractControllerTest<IcebergNamespaceApiController> {

  private static final String TEST_PLATFORM = "test-platform";
  private Namespace namespace;
  private String namespaceString;

  @Override
  public void onSetup() {
    setupNamespace();
  }

  @Override
  protected IcebergNamespaceApiController newController() {
    return new IcebergNamespaceApiController();
  }

  private void setupNamespace() {
    Namespace namespaceParent = Namespace.of("db");
    namespace = Namespace.of("db", "schema");
    namespaceString = "db\u001fschema";

    Urn parentContainerUrn = Utils.containerUrn(TEST_PLATFORM, namespaceParent);
    Urn containerUrn = Utils.containerUrn(TEST_PLATFORM, namespace);

    doReturn(true).when(entityService).exists(any(OperationContext.class), eq(parentContainerUrn));

    ContainerProperties containerProperties = new ContainerProperties();
    when(entityService.getLatestAspect(
            any(OperationContext.class), eq(containerUrn), eq(CONTAINER_PROPERTIES_ASPECT_NAME)))
        .thenReturn(containerProperties);
  }

  @Test
  public void testGetNamespace() throws Exception {
    GetNamespaceResponse response =
        controller.getNamespace(request, TEST_PLATFORM, namespaceString);

    assertNotNull(response);
    assertEquals(response.namespace(), namespace);
  }

  @Test
  public void testCreateNamespace() throws Exception {
    CreateNamespaceRequest createRequest =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();

    CreateNamespaceResponse response =
        controller.createNamespace(request, TEST_PLATFORM, createRequest);

    assertNotNull(response);
    assertEquals(response.namespace(), namespace);
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testCreateNamespaceUnauthorized() throws Exception {
    setupDefaultAuthorization(false);
    CreateNamespaceRequest createRequest =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();

    controller.createNamespace(request, TEST_PLATFORM, createRequest);
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testGetNamespaceNonexistent() throws Exception {
    String missingNamespaceString = "db\u001fschema2";
    Namespace missingNamespace = Namespace.of("db", "schema2");
    Urn containerUrn = Utils.containerUrn(TEST_PLATFORM, missingNamespace);

    doReturn(false).when(entityService).exists(any(OperationContext.class), eq(containerUrn));
    controller.getNamespace(request, TEST_PLATFORM, missingNamespaceString);
  }
}
