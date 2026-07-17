package com.linkedin.datahub.graphql.types.dataplatform.mappers;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.generated.DataPlatformProperties;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import org.testng.annotations.Test;

public class DataPlatformPropertiesMapperTest {

  @Test
  public void testLogicalFlagMapped() {
    DataPlatformInfo input =
        new DataPlatformInfo()
            .setName("logical")
            .setType(PlatformType.OTHERS)
            .setDatasetNameDelimiter(".")
            .setLogical(true);

    DataPlatformProperties result = DataPlatformPropertiesMapper.map(null, input);

    assertTrue(result.getLogical());
  }

  @Test
  public void testLogicalFlagAbsentWhenNotSet() {
    DataPlatformInfo input =
        new DataPlatformInfo()
            .setName("hive")
            .setType(PlatformType.RELATIONAL_DB)
            .setDatasetNameDelimiter(".");

    DataPlatformProperties result = DataPlatformPropertiesMapper.map(null, input);

    assertNull(result.getLogical());
  }
}
