package com.linkedin.datahub.graphql.types.dataobject;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataObject;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataobject.DataObjectProperties;
import com.linkedin.dataobject.ObjectStorageProperties;
import com.linkedin.dataobject.ParentDataObject;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataObjectKey;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataObjectMapperTest {

  private static final String TEST_DATA_OBJECT_URN =
      "urn:li:dataObject:(urn:li:dataPlatform:s3,b/clip.mp4,PROD)";
  private static final String TEST_PLATFORM_URN = "urn:li:dataPlatform:s3";
  private static final String TEST_PARENT_URN =
      "urn:li:dataObject:(urn:li:dataPlatform:s3,b/clip.mp4,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testuser";
  private static final Long TEST_TIMESTAMP = 1640995200000L;

  private Urn dataObjectUrn;
  private Urn actorUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    dataObjectUrn = Urn.createFromString(TEST_DATA_OBJECT_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapDataObjectProperties() {
    EntityResponse response = createBasicEntityResponse();

    DataObjectProperties properties = new DataObjectProperties();
    properties.setName("clip.mp4");
    properties.setQualifiedName("b/clip.mp4");
    properties.setDescription("A video clip");
    properties.setExternalUrl(new Url("https://example.com/b/clip.mp4"));
    StringMap customProperties = new StringMap();
    customProperties.put("region", "us-east-1");
    properties.setCustomProperties(customProperties);
    properties.setCreated(new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn));
    properties.setLastModified(new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn));
    addAspectToResponse(response, DATA_OBJECT_PROPERTIES_ASPECT_NAME, properties);

    ObjectStorageProperties storage = new ObjectStorageProperties();
    storage.setMimeType("video/mp4");
    storage.setSizeBytes(1024L);
    storage.setStorageClass("STANDARD");
    addAspectToResponse(response, OBJECT_STORAGE_PROPERTIES_ASPECT_NAME, storage);

    DataObject result = DataObjectMapper.map(null, response);

    assertEquals(result.getType(), EntityType.DATA_OBJECT);
    assertEquals(result.getUrn(), TEST_DATA_OBJECT_URN);
    assertEquals(result.getName(), "clip.mp4");

    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), "clip.mp4");
    assertEquals(result.getProperties().getQualifiedName(), "b/clip.mp4");
    assertEquals(result.getProperties().getDescription(), "A video clip");
    assertEquals(result.getProperties().getExternalUrl(), "https://example.com/b/clip.mp4");
    assertNotNull(result.getProperties().getCustomProperties());
    assertEquals(result.getProperties().getCustomProperties().size(), 1);
    assertNotNull(result.getProperties().getCreated());
    assertEquals(result.getProperties().getCreated().getTime(), TEST_TIMESTAMP.longValue());
    assertEquals(result.getProperties().getCreated().getActor(), TEST_ACTOR_URN);

    assertNotNull(result.getStorage());
    assertEquals(result.getStorage().getMimeType(), "video/mp4");
    assertEquals(result.getStorage().getSizeBytes(), Long.valueOf(1024L));
    assertEquals(result.getStorage().getStorageClass(), "STANDARD");
  }

  @Test
  public void testMapDataObjectKeyPopulatesPlatform() {
    EntityResponse response = createBasicEntityResponse();

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getPlatform());
    assertEquals(result.getPlatform().getUrn(), TEST_PLATFORM_URN);
    assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);
    // name comes from the key when no properties aspect is present
    assertEquals(result.getName(), "b/clip.mp4");
  }

  @Test
  public void testMapParentDataObjectStub() throws URISyntaxException {
    EntityResponse response = createBasicEntityResponse();

    ParentDataObject parent = new ParentDataObject();
    parent.setObject(Urn.createFromString(TEST_PARENT_URN));
    addAspectToResponse(response, PARENT_DATA_OBJECT_ASPECT_NAME, parent);

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getParent());
    assertEquals(result.getParent().getUrn(), TEST_PARENT_URN);
    assertEquals(result.getParent().getType(), EntityType.DATA_OBJECT);
  }

  @Test
  public void testMapStatusRemovedSetsExistsFalse() {
    EntityResponse response = createBasicEntityResponse();

    Status status = new Status();
    status.setRemoved(true);
    addAspectToResponse(response, STATUS_ASPECT_NAME, status);

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getExists());
    assertFalse(result.getExists());
  }

  @Test
  public void testMapPopulatesDeprecatedGlobalTagsAlias() {
    // The shared sidebar tags section reads the deprecated globalTags alias, so DataObject must
    // populate both tags and globalTags from the GlobalTags aspect (same pattern as Dataset).
    EntityResponse response = createBasicEntityResponse();

    GlobalTags globalTags = new GlobalTags();
    globalTags.setTags(
        new TagAssociationArray(
            com.google.common.collect.ImmutableList.of(
                new TagAssociation().setTag(new TagUrn("media")))));
    addAspectToResponse(response, GLOBAL_TAGS_ASPECT_NAME, globalTags);

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(dataObjectUrn)))
          .thenReturn(true);

      DataObject result = DataObjectMapper.map(mockQueryContext, response);

      assertNotNull(result.getTags());
      assertNotNull(result.getGlobalTags());
      assertSame(result.getGlobalTags(), result.getTags());
    }
  }

  @Test
  public void testMapWithRestrictedAccess() {
    EntityResponse response = createBasicEntityResponse();

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(dataObjectUrn)))
          .thenReturn(false);

      DataObject restricted = new DataObject();
      authUtilsMock
          .when(
              () -> AuthorizationUtils.restrictEntity(any(DataObject.class), eq(DataObject.class)))
          .thenReturn(restricted);

      DataObject result = DataObjectMapper.map(mockQueryContext, response);

      assertSame(result, restricted);
    }
  }

  private EntityResponse createBasicEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(dataObjectUrn);

    DataObjectKey key = new DataObjectKey();
    try {
      key.setPlatform(Urn.createFromString(TEST_PLATFORM_URN));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    key.setName("b/clip.mp4");
    key.setOrigin(com.linkedin.common.FabricType.PROD);

    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(key.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(DATA_OBJECT_KEY_ASPECT_NAME, keyAspect);

    entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    return entityResponse;
  }

  private void addAspectToResponse(
      EntityResponse entityResponse, String aspectName, Object aspectData) {
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(((com.linkedin.data.template.RecordTemplate) aspectData).data()));
    entityResponse.getAspects().put(aspectName, aspect);
  }
}
