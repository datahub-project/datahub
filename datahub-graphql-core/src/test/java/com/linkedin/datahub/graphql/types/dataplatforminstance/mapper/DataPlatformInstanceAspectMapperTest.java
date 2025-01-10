package com.linkedin.datahub.graphql.types.dataplatforminstance.mapper;

import static org.testng.Assert.*;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import org.testng.annotations.Test;

public class DataPlatformInstanceAspectMapperTest {

  private static final String TEST_PLATFORM = "hive";
  private static final String TEST_INSTANCE = "prod";
  private static final String TEST_PLATFORM_URN = "urn:li:dataPlatform:" + TEST_PLATFORM;
  private static final String TEST_INSTANCE_URN =
      String.format(
          "urn:li:dataPlatformInstance:(urn:li:dataPlatform:%s,%s)", TEST_PLATFORM, TEST_INSTANCE);

  @Test
  public void testMapWithInstance() throws Exception {
    // Create test input
    com.linkedin.common.DataPlatformInstance input = new com.linkedin.common.DataPlatformInstance();
    DataPlatformUrn platformUrn = new DataPlatformUrn(TEST_PLATFORM);
    Urn instanceUrn = Urn.createFromString(TEST_INSTANCE_URN);

    input.setPlatform(platformUrn);
    input.setInstance(instanceUrn);

    // Map and verify
    DataPlatformInstance result = DataPlatformInstanceAspectMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getType(), EntityType.DATA_PLATFORM_INSTANCE);
    assertEquals(result.getUrn(), TEST_INSTANCE_URN);

    // Verify platform mapping
    assertNotNull(result.getPlatform());
    assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);
    assertEquals(result.getPlatform().getUrn(), TEST_PLATFORM_URN);
  }

  @Test
  public void testMapWithoutInstance() throws Exception {
    // Create test input with only platform
    com.linkedin.common.DataPlatformInstance input = new com.linkedin.common.DataPlatformInstance();
    DataPlatformUrn platformUrn = new DataPlatformUrn(TEST_PLATFORM);
    input.setPlatform(platformUrn);

    // Map and verify
    DataPlatformInstance result = DataPlatformInstanceAspectMapper.map(null, input);

    assertNotNull(result);
    assertNull(result.getType()); // Type should be null when no instance
    assertNull(result.getUrn()); // URN should be null when no instance

    // Verify platform is still mapped correctly
    assertNotNull(result.getPlatform());
    assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);
    assertEquals(result.getPlatform().getUrn(), TEST_PLATFORM_URN);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testMapNull() {
    DataPlatformInstanceAspectMapper.map(null, null);
  }

  @Test
  public void testSingleton() {
    assertNotNull(DataPlatformInstanceAspectMapper.INSTANCE);
    assertSame(
        DataPlatformInstanceAspectMapper.INSTANCE, DataPlatformInstanceAspectMapper.INSTANCE);
  }
}
