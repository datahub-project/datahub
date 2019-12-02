package com.linkedin.metadata.dao;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
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
}
