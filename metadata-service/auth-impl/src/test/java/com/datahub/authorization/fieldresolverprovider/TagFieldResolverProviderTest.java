package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TagFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<TagFieldResolverProvider> {

  private static final String TAG_URN = "urn:li:tag:test";
  private static final String RESOURCE_URN =
      "urn:li:dataset:(urn:li:dataPlatform:s3,test-platform-instance.testDataset,PROD)";
  private static final EntitySpec RESOURCE_SPEC = new EntitySpec(DATASET_ENTITY_NAME, RESOURCE_URN);

  @Mock private SystemEntityClient entityClientMock;

  private TagFieldResolverProvider tagFieldResolverProvider;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
    tagFieldResolverProvider = buildFieldResolverProvider();
  }

  @Override
  protected TagFieldResolverProvider buildFieldResolverProvider() {
    return new TagFieldResolverProvider(entityClientMock);
  }

  @Test
  public void shouldReturnTagType() {
    assertEquals(EntityFieldType.TAG, tagFieldResolverProvider.getFieldTypes().get(0));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResponseIsNull()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(null);

    var result =
        tagFieldResolverProvider.getFieldResolver(mock(OperationContext.class), RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(entityClientMock, times(1))
        .getV2(
            any(),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResourceHasNoTag()
      throws RemoteInvocationException, URISyntaxException {
    var entityResponseMock = mock(EntityResponse.class);
    when(entityResponseMock.getAspects()).thenReturn(new EnvelopedAspectMap());
    when(entityClientMock.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        tagFieldResolverProvider.getFieldResolver(mock(OperationContext.class), RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(entityClientMock, times(1))
        .getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenThereIsAnException()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    var result =
        tagFieldResolverProvider.getFieldResolver(mock(OperationContext.class), RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(entityClientMock, times(1))
        .getV2(
            any(),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnFieldValueWithTagOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var tagAssociation = new TagAssociation();
    tagAssociation.setTag(new TagUrn("test"));
    var tags = new TagAssociationArray(tagAssociation);
    var globalTags = new GlobalTags().setTags(tags);
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        GLOBAL_TAGS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(globalTags.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        tagFieldResolverProvider.getFieldResolver(mock(OperationContext.class), RESOURCE_SPEC);

    assertEquals(Set.of(TAG_URN), result.getFieldValuesFuture().join().getValues());
    verify(entityClientMock, times(1))
        .getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(Collections.singleton(GLOBAL_TAGS_ASPECT_NAME)));
  }
}
