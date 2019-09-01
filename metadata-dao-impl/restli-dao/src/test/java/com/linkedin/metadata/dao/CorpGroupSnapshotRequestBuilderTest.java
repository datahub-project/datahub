package com.linkedin.metadata.dao;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CorpGroupSnapshotRequestBuilderTest extends BaseSnapshotRequestBuilderTest {

  @Test
  public void testUrnClass() {
    CorpGroupSnapshotRequestBuilder builder = new CorpGroupSnapshotRequestBuilder();

    assertEquals(builder.urnClass(), CorpGroupUrn.class);
  }

  @Test
  public void testGetRequest() {
    CorpGroupSnapshotRequestBuilder builder = new CorpGroupSnapshotRequestBuilder();
    String aspectName = ModelUtils.getAspectName(Ownership.class);
    CorpGroupUrn urn = new CorpGroupUrn("foo");

    GetRequest<CorpGroupSnapshot> request = builder.getRequest(aspectName, urn, 1);

    Map<String, Object> keyPaths = Collections.singletonMap("corpGroup",
        new ComplexResourceKey<>(new CorpGroupKey().setName("foo"), new EmptyRecord()));
    validateRequest(request, "corpGroups/{corpGroup}/snapshot", keyPaths, aspectName, 1);
  }

  @Test
  public void testGetValidBuilder() {
    CorpGroupUrn urn = new CorpGroupUrn("foo");
    assertEquals(RequestBuilders.getBuilder(urn).getClass(), CorpGroupSnapshotRequestBuilder.class);
  }
}