package com.linkedin.metadata.dao;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserKey;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CorpUserSnapshotRequestBuilderTest extends BaseSnapshotRequestBuilderTest {

  @Test
  public void testUrnClass() {
    CorpUserSnapshotRequestBuilder builder = new CorpUserSnapshotRequestBuilder();

    assertEquals(builder.urnClass(), CorpuserUrn.class);
  }

  @Test
  public void testGetRequest() {
    CorpUserSnapshotRequestBuilder builder = new CorpUserSnapshotRequestBuilder();
    String aspectName = ModelUtils.getAspectName(Ownership.class);
    CorpuserUrn urn = new CorpuserUrn("foo");

    GetRequest<CorpUserSnapshot> request = builder.getRequest(aspectName, urn, 1);

    Map<String, Object> keyPaths = Collections.singletonMap("corpUser",
        new ComplexResourceKey<>(new CorpUserKey().setName("foo"), new EmptyRecord()));
    validateRequest(request, "corpUsers/{corpUser}/snapshot", keyPaths, aspectName, 1);
  }

  @Test
  public void testGetValidBuilder() {
    CorpuserUrn urn = new CorpuserUrn("foo");
    assertEquals(RequestBuilders.getBuilder(urn).getClass(), CorpUserSnapshotRequestBuilder.class);
  }
}