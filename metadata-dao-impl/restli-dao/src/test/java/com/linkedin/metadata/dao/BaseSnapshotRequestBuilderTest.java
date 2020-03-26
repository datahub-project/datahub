package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;


public class BaseSnapshotRequestBuilderTest {

  protected <SNAPSHOT extends RecordTemplate> void validateRequest(GetRequest<SNAPSHOT> request, String baseUriTemplate,
      Map<String, Object> pathKeys, String aspectName, long version) {

    ComplexResourceKey<SnapshotKey, EmptyRecord> id =
        (ComplexResourceKey<SnapshotKey, EmptyRecord>) request.getObjectId();
    List<AspectVersion> aspectVersions = id.getKey().getAspectVersions();

    assertEquals(aspectVersions.size(), 1);
    assertEquals(aspectVersions.get(0).getAspect(), aspectName);
    assertEquals(aspectVersions.get(0).getVersion(), Long.valueOf(version));
    assertEquals(request.getBaseUriTemplate(), baseUriTemplate);
    assertEquals(request.getPathKeys(), pathKeys);
  }
}
