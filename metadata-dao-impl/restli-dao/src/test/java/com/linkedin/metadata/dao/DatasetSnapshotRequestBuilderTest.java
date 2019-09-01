package com.linkedin.metadata.dao;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.ComplianceInfo;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DatasetSnapshotRequestBuilderTest extends BaseSnapshotRequestBuilderTest {

  @Test
  public void testUrnClass() {
    DatasetSnapshotRequestBuilder builder = new DatasetSnapshotRequestBuilder();

    assertEquals(builder.urnClass(), DatasetUrn.class);
  }

  @Test
  public void testGetRequest() {
    DatasetSnapshotRequestBuilder builder = new DatasetSnapshotRequestBuilder();
    String aspectName = ModelUtils.getAspectName(ComplianceInfo.class);
    DatasetUrn urn = new DatasetUrn(new DataPlatformUrn("mysql"), "QUEUING.bar", FabricType.EI);

    GetRequest<DatasetSnapshot> request = builder.getRequest(aspectName, urn, 0);

    Map<String, Object> keyPaths = Collections.singletonMap("key", new ComplexResourceKey<>(
        new DatasetKey().setPlatform(new DataPlatformUrn("mysql")).setName("QUEUING.bar").setOrigin(FabricType.EI),
        new EmptyRecord()));
    validateRequest(request, "datasets/{key}/snapshot", keyPaths, aspectName, 0);
  }
}
