package com.linkedin.metadata.system_info.collectors;

import static com.linkedin.metadata.system_info.SystemInfoDtos.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.system_info.SystemInfoDtos.*;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SpringComponentsCollector {

  public static final String SYSTEM_INFO_ENDPOINT = "/openapi/v1/system-info";

  private final OperationContext systemOperationContext;
  private final GitVersion gitVersion;
  private final PropertiesCollector propertiesCollector;

  @Value("${maeConsumerUrl}")
  private String maeConsumerUrl;

  @Value("${mceConsumerUrl}")
  private String mceConsumerUrl;

  public SpringComponentsInfo collect(ExecutorService executorService) {
    List<CompletableFuture<ComponentInfo>> futures =
        Arrays.asList(
            CompletableFuture.supplyAsync(this::getGmsInfo, executorService),
            CompletableFuture.supplyAsync(this::getMaeConsumerInfo, executorService),
            CompletableFuture.supplyAsync(this::getMceConsumerInfo, executorService));

    try {
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.get(10, TimeUnit.SECONDS);

      return SpringComponentsInfo.builder()
          .gms(futures.get(0).getNow(createErrorComponent(GMS_COMPONENT_NAME)))
          .maeConsumer(futures.get(1).getNow(createErrorComponent(MAE_COMPONENT_NAME)))
          .mceConsumer(futures.get(2).getNow(createErrorComponent(MCE_COMPONENT_NAME)))
          .build();
    } catch (Exception e) {
      log.error("Error getting spring components info", e);
      return SpringComponentsInfo.builder()
          .gms(createErrorComponent(GMS_COMPONENT_NAME))
          .maeConsumer(createErrorComponent(MAE_COMPONENT_NAME))
          .mceConsumer(createErrorComponent(MCE_COMPONENT_NAME))
          .build();
    }
  }

  private ComponentInfo getGmsInfo() {
    try {
      Map<String, Object> properties = propertiesCollector.getPropertiesAsMap();

      return ComponentInfo.builder()
          .name(GMS_COMPONENT_NAME)
          .status(ComponentStatus.AVAILABLE)
          .version(gitVersion.getVersion())
          .properties(properties)
          .build();
    } catch (Exception e) {
      return createErrorComponent(GMS_COMPONENT_NAME, e);
    }
  }

  private ComponentInfo getMaeConsumerInfo() {
    return fetchRemoteComponentInfo(MAE_COMPONENT_NAME, maeConsumerUrl, MAE_COMPONENT_KEY);
  }

  private ComponentInfo getMceConsumerInfo() {
    return fetchRemoteComponentInfo(MCE_COMPONENT_NAME, mceConsumerUrl, MCE_COMPONENT_KEY);
  }

  private ComponentInfo fetchRemoteComponentInfo(
      String name, String baseUrl, String componentField) {
    try {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(baseUrl + SYSTEM_INFO_ENDPOINT))
              .timeout(Duration.ofSeconds(5))
              .GET()
              .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        ObjectMapper mapper = systemOperationContext.getObjectMapper();
        SpringComponentsInfo springInfo =
            mapper.readValue(response.body(), SpringComponentsInfo.class);

        switch (componentField) {
          case MAE_COMPONENT_KEY:
            return springInfo.getMaeConsumer();
          case MCE_COMPONENT_KEY:
            return springInfo.getMceConsumer();
          case GMS_COMPONENT_KEY:
            return springInfo.getGms();
          default:
            throw new IllegalArgumentException("Unknown component: " + componentField);
        }
      }

      return createUnavailableComponent(name);
    } catch (Exception e) {
      return createErrorComponent(name, e);
    }
  }

  private ComponentInfo createErrorComponent(String name) {
    return ComponentInfo.builder()
        .name(name)
        .status(ComponentStatus.ERROR)
        .errorMessage("Failed to retrieve component info")
        .build();
  }

  private ComponentInfo createErrorComponent(String name, Exception e) {
    return ComponentInfo.builder()
        .name(name)
        .status(ComponentStatus.ERROR)
        .errorMessage("Failed to connect: " + e.getMessage())
        .build();
  }

  private ComponentInfo createUnavailableComponent(String name) {
    return ComponentInfo.builder().name(name).status(ComponentStatus.UNAVAILABLE).build();
  }
}
