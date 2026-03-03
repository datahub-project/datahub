package com.linkedin.datahub.graphql.types.ingestion;

import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.entity.EntityResponse;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
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
}
