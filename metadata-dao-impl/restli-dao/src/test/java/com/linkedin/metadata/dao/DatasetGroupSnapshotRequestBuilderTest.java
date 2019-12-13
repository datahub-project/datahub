package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.datasetGroup.DatasetGroupKey;
import com.linkedin.datasetGroup.DatasetGroupMembership;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DatasetGroupSnapshot;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DatasetGroupSnapshotRequestBuilderTest extends BaseSnapshotRequestBuilderTest {

  @Test
  public void testUrnClass() {
    DatasetGroupSnapshotRequestBuilder builder = new DatasetGroupSnapshotRequestBuilder();

    assertEquals(builder.urnClass(), DatasetGroupUrn.class);
  }

  @Test
  public void testGetRequest() {
    DatasetGroupSnapshotRequestBuilder builder = new DatasetGroupSnapshotRequestBuilder();
    String aspectName = ModelUtils.getAspectName(DatasetGroupMembership.class);
    DatasetGroupUrn urn = new DatasetGroupUrn("foo", "bar");

    GetRequest<DatasetGroupSnapshot> request =
        (GetRequest<DatasetGroupSnapshot>) builder.getRequest(aspectName, urn, 1);

    Map<String, Object> keyPaths = Collections.singletonMap("key", new ComplexResourceKey<>(
        new DatasetGroupKey().setNamespace(urn.getNamespaceEntity()).setName(urn.getNameEntity()), new EmptyRecord()));
    validateRequest(request, "datasetGroups/{key}/snapshot", keyPaths, aspectName, 1);
  }
}
