package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.common.RestConstants;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntitySnapshot;
import java.util.Map;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.restli.RestliConstants.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class BaseActionRequestBuilderTest {

  private static final Map<String, Object> PATH_KEYS = ImmutableMap.of("key", Long.valueOf(123));

  private static class RequestBuilderWithoutPathKey extends BaseActionRequestBuilder<EntitySnapshot, Urn> {

    public RequestBuilderWithoutPathKey() {
      super(EntitySnapshot.class, Urn.class, "entities");
    }
  }

  private static class RequestBuilderWithPathKey extends BaseActionRequestBuilder<EntitySnapshot, Urn> {

    public RequestBuilderWithPathKey() {
      super(EntitySnapshot.class, Urn.class, "entities/{key}/foo");
    }

    protected Map<String, Object> pathKeys(Urn urn) {
      return PATH_KEYS;
    }
  }

  @Test
  public void testGetRequest() {
    Urn urn = makeUrn(1);
    BaseActionRequestBuilder<EntitySnapshot, Urn> builder = new RequestBuilderWithoutPathKey();
    String aspectFooName = ModelUtils.getAspectName(AspectFoo.class);

    Request<EntitySnapshot> request = builder.getRequest(aspectFooName, urn, LATEST_VERSION);

    assertGetSnapshotRequest(request, "entities", ImmutableMap.of(), urn, aspectFooName);
  }

  @Test
  public void testGetRequestWithPathKey() {
    Urn urn = makeUrn(1);
    BaseActionRequestBuilder<EntitySnapshot, Urn> builder = new RequestBuilderWithPathKey();
    String aspectFooName = ModelUtils.getAspectName(AspectFoo.class);

    Request<EntitySnapshot> request = builder.getRequest(aspectFooName, urn, LATEST_VERSION);

    assertGetSnapshotRequest(request, "entities/{key}/foo", PATH_KEYS, urn, aspectFooName);
  }

  private void assertGetSnapshotRequest(Request<EntitySnapshot> request, String baseUriTemplate,
      Map<String, Object> pathKeys, Urn urn, String aspectName) {

    assertEquals(request.getQueryParamsObjects().get(RestConstants.ACTION_PARAM), ACTION_GET_SNAPSHOT);
    assertEquals(request.getBaseUriTemplate(), baseUriTemplate);
    assertEquals(request.getPathKeys(), pathKeys);

    RecordTemplate input = request.getInputRecord();
    assertEquals(RecordUtils.getRecordTemplateField(input, PARAM_URN, Urn.class), urn);
    assertEquals(RecordUtils.getRecordTemplateField(input, PARAM_ASPECTS, StringArray.class),
        new StringArray(ImmutableList.of(aspectName)));
  }

  @Test
  public void testCreateRequest() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn,
        ImmutableList.of(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo)));
    BaseActionRequestBuilder<EntitySnapshot, Urn> builder = new RequestBuilderWithoutPathKey();

    Request request = builder.createRequest(urn, snapshot);

    assertIngestRequest(request, "entities", ImmutableMap.of(), snapshot);
  }

  @Test
  public void testCreateRequestWithPathKey() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn,
        ImmutableList.of(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo)));
    BaseActionRequestBuilder<EntitySnapshot, Urn> builder = new RequestBuilderWithPathKey();

    Request request = builder.createRequest(urn, snapshot);

    assertIngestRequest(request, "entities/{key}/foo", PATH_KEYS, snapshot);
  }

  private void assertIngestRequest(Request request, String baseUriTemplate, Map<String, Object> pathKeys,
      EntitySnapshot snapshot) {

    assertEquals(request.getQueryParamsObjects().get(RestConstants.ACTION_PARAM), ACTION_INGEST);
    assertEquals(request.getBaseUriTemplate(), baseUriTemplate);

    RecordTemplate input = request.getInputRecord();
    assertEquals(RecordUtils.getRecordTemplateField(input, PARAM_SNAPSHOT, EntitySnapshot.class), snapshot);
  }
}
