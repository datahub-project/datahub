package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<ContainerFieldResolverProvider> {

  private static final String CONTAINER_URN = "urn:li:container:fooContainer";
  private static final String RESOURCE_URN =
      "urn:li:dataset:(urn:li:dataPlatform:s3,test-platform-instance.testDataset,PROD)";
  private static final EntitySpec RESOURCE_SPEC = new EntitySpec(DATASET_ENTITY_NAME, RESOURCE_URN);

  @Mock private SystemEntityClient entityClientMock;

  private OperationContext systemOperationContext;

  private ContainerFieldResolverProvider containerFieldResolverProvider;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
    containerFieldResolverProvider = buildFieldResolverProvider();
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Override
  protected ContainerFieldResolverProvider buildFieldResolverProvider() {
    return new ContainerFieldResolverProvider(entityClientMock);
  }

  @Test
  public void shouldReturnContainerType() {
    assertEquals(EntityFieldType.CONTAINER, containerFieldResolverProvider.getFieldTypes().get(0));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResponseIsNull()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(null);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResourceHasNoContainer()
      throws RemoteInvocationException, URISyntaxException {
    var entityResponseMock = mock(EntityResponse.class);
    when(entityResponseMock.getAspects()).thenReturn(new EnvelopedAspectMap());
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenThereIsAnException()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
  }

  @Test
  public void shouldReturnFieldValueWithContainerOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var container = new Container().setContainer(Urn.createFromString(CONTAINER_URN));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertEquals(result.getFieldValuesFuture().join().getValues(), Set.of(CONTAINER_URN));
  }

  @Test
  public void shouldReturnFieldValueWithAncestorContainersOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var container = new Container().setContainer(Urn.createFromString(CONTAINER_URN));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    container = new Container().setContainer(Urn.createFromString(CONTAINER_URN + "_parent"));
    entityResponseMock = mock(EntityResponse.class);
    envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(CONTAINER_ENTITY_NAME),
            eq(Urn.createFromString(CONTAINER_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertEquals(
        result.getFieldValuesFuture().join().getValues(),
        Set.of(CONTAINER_URN, CONTAINER_URN + "_parent"));
  }

  @Test
  public void shouldReturnFieldValueAsContainerItself()
      throws RemoteInvocationException, URISyntaxException {

    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(CONTAINER_ENTITY_NAME),
            eq(Urn.createFromString(CONTAINER_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(null);

    var result =
        containerFieldResolverProvider.getFieldResolver(
            systemOperationContext, new EntitySpec(CONTAINER_ENTITY_NAME, CONTAINER_URN));

    assertEquals(result.getFieldValuesFuture().join().getValues(), Set.of(CONTAINER_URN));
  }
}
