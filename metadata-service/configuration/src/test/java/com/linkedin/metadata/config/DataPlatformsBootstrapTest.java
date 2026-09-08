package com.linkedin.metadata.config;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class DataPlatformsBootstrapTest {

  private static final String ABS_URN = "urn:li:dataPlatform:abs";

  @Test
  public void testAzureBlobStoragePlatformBootstrapMetadata() throws IOException {
    List<Map<String, Object>> platforms;
    try (InputStream input =
        getClass().getClassLoader().getResourceAsStream("bootstrap_mcps/data-platforms.yaml")) {
      platforms =
          new YAMLMapper().readValue(input, new TypeReference<List<Map<String, Object>>>() {});
    }

    Map<String, Object> absPlatform =
        platforms.stream()
            .filter(platform -> ABS_URN.equals(platform.get("entityUrn")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("ABS data platform bootstrap is missing"));

    assertEquals(absPlatform.get("entityType"), "dataPlatform");
    assertEquals(absPlatform.get("aspectName"), "dataPlatformInfo");
    assertEquals(absPlatform.get("changeType"), "UPSERT");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspect = (Map<String, Object>) absPlatform.get("aspect");
    assertEquals(aspect.get("datasetNameDelimiter"), "/");
    assertEquals(aspect.get("name"), "abs");
    assertEquals(aspect.get("displayName"), "Azure Blob Storage");
    assertEquals(aspect.get("type"), "FILE_SYSTEM");
    assertEquals(aspect.get("logoUrl"), "assets/platforms/azureblobstoragelogo.svg");
  }
}
