package io.datahubproject.openapi.openlineage.config;

import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import java.io.IOException;
import java.io.InputStream;
import org.springdoc.core.customizers.OpenApiCustomizer;

public final class PinnedOpenLineageOpenApiCustomizer implements OpenApiCustomizer {
  private static final String CONTRACT_RESOURCE = "openlineage/openlineage.json";

  private final OpenAPI contract;

  public PinnedOpenLineageOpenApiCustomizer() {
    contract = loadContract();
  }

  @Override
  public void customise(OpenAPI openApi) {
    openApi.setOpenapi(contract.getOpenapi());
    openApi.setSpecVersion(contract.getSpecVersion());
    openApi.setInfo(contract.getInfo());
    openApi.setExternalDocs(contract.getExternalDocs());
    openApi.setSecurity(contract.getSecurity());
    openApi.setTags(contract.getTags());
    openApi.setServers(contract.getServers());
    openApi.setPaths(contract.getPaths());
    openApi.setComponents(contract.getComponents());
    openApi.setExtensions(contract.getExtensions());
  }

  private static OpenAPI loadContract() {
    try (InputStream input =
        PinnedOpenLineageOpenApiCustomizer.class
            .getClassLoader()
            .getResourceAsStream(CONTRACT_RESOURCE)) {
      if (input == null) {
        throw new IllegalStateException("Missing OpenLineage OpenAPI contract");
      }
      return Json.mapper().readValue(input, OpenAPI.class);
    } catch (IOException exception) {
      throw new IllegalStateException("Unable to load OpenLineage OpenAPI contract", exception);
    }
  }
}
