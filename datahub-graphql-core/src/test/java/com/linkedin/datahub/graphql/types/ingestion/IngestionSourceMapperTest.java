package com.linkedin.datahub.graphql.types.ingestion;

import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.entity.EntityResponse;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.ingestion.DataHubIngestionSourceSource;
import com.linkedin.ingestion.DataHubIngestionSourceSourceType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import org.testng.annotations.Test;

public class IngestionSourceMapperTest {
  private static final String TEST_INGESTION_SOURCE_URN = "urn:li:dataHubIngestionSource:id-1";

  @Test
  public void testGetSuccess() throws Exception {
    EntityResponse entityResponse =
        getEntityResponse().setUrn(Urn.createFromString(TEST_INGESTION_SOURCE_URN));
    DataHubIngestionSourceSchedule schedule = getSchedule("UTC", "* * * * *");
    DataHubIngestionSourceConfig config = getConfig("0.8.18", "{}", "id");
    DataHubIngestionSourceInfo ingestionSourceInfo =
        getIngestionSourceInfo("Source", "mysql", schedule, config);
    addAspect(entityResponse, Constants.INGESTION_INFO_ASPECT_NAME, ingestionSourceInfo);

    IngestionSource ingestionSource = IngestionSourceMapper.map(null, entityResponse);

    assertNotNull(ingestionSource);
    assertEquals(ingestionSource.getUrn(), TEST_INGESTION_SOURCE_URN);
    verifyIngestionSourceInfo(ingestionSource, ingestionSourceInfo);
  }

  @Test
  public void testReturnNullWhenNoInfoAspect() throws Exception {
    EntityResponse entityResponse =
        getEntityResponse().setUrn(Urn.createFromString(TEST_INGESTION_SOURCE_URN));

    IngestionSource ingestionSource = IngestionSourceMapper.map(null, entityResponse);

    assertNull(ingestionSource);
  }

  @Test
  public void testSourceTypeFromNewerVersionLeavesOutSource() throws Exception {
    // Read after a rollback: the source type is required, so one only a newer version defines
    // leaves out the source; the ingestion source is still returned.
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    try {
      IngestionSource ingestionSource = IngestionSourceMapper.map(null, sourceWithNewerType());
      assertNotNull(ingestionSource);
      assertNull(ingestionSource.getSource());
    } finally {
      UnknownDataHandlingsForTests.reset();
    }
  }

  @Test
  public void testSourceTypeFromNewerVersionStrictHandlingFails() throws Exception {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    try {
      EntityResponse response = sourceWithNewerType();
      assertThrows(IllegalArgumentException.class, () -> IngestionSourceMapper.map(null, response));
    } finally {
      UnknownDataHandlingsForTests.reset();
    }
  }

  private static EntityResponse sourceWithNewerType() throws Exception {
    EntityResponse entityResponse =
        getEntityResponse().setUrn(Urn.createFromString(TEST_INGESTION_SOURCE_URN));
    DataHubIngestionSourceInfo info =
        getIngestionSourceInfo(
            "Source", "mysql", getSchedule("UTC", "* * * * *"), getConfig("0.8.18", "{}", "id"));
    info.setSource(
        new DataHubIngestionSourceSource().setType(DataHubIngestionSourceSourceType.SYSTEM));
    info.getSource().data().put("type", "SOURCE_TYPE_FROM_NEWER_BUILD");
    addAspect(entityResponse, Constants.INGESTION_INFO_ASPECT_NAME, info);
    return entityResponse;
  }
}
