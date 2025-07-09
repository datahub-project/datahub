package io.datahubproject.openapi.operations.v1;

import static com.linkedin.metadata.system_info.collectors.SpringComponentsCollector.SYSTEM_INFO_ENDPOINT;

import com.linkedin.metadata.system_info.SystemInfoDtos;
import com.linkedin.metadata.system_info.SystemInfoService;
import io.datahubproject.metadata.context.OperationContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(SYSTEM_INFO_ENDPOINT)
@Slf4j
@Tag(name = "System Information", description = "APIs for retrieving DataHub system information")
public class SystemInfoController {
  @Autowired private OperationContext systemOperationContext;
  @Autowired private SystemInfoService systemInfoService;

  @Operation(
      summary = "Get all system information",
      description = "Retrieves comprehensive system information from all DataHub components")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved system information",
            content = @Content(schema = @Schema(implementation = SystemInfoDtos.SystemInfo.class))),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  @GetMapping(value = "/all", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.SystemInfo> getAllSystemInfo() {
    SystemInfoDtos.SystemInfo systemInfo =
        SystemInfoDtos.SystemInfo.builder()
            .timestamp(LocalDateTime.now())
            .springComponents(systemInfoService.getSpringComponentsInfo())
            .kubernetesInfo(systemInfoService.getKubernetesInfo())
            .storageInfo(systemInfoService.getStorageInfo())
            .kafkaInfo(systemInfoService.getKafkaInfo())
            .build();

    return ResponseEntity.ok(systemInfo);
  }

  @Operation(
      summary = "Download all system information as ZIP",
      description =
          "Downloads comprehensive system information as a ZIP file containing JSON files")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Successfully generated ZIP file"),
        @ApiResponse(responseCode = "500", description = "Error generating ZIP file")
      })
  @GetMapping(value = "/download", produces = "application/zip")
  public ResponseEntity<Resource> downloadSystemInfoAsZip() {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ZipOutputStream zos = new ZipOutputStream(baos);

      // Add Spring components info
      SystemInfoDtos.SpringComponentsInfo springInfo = systemInfoService.getSpringComponentsInfo();
      addToZip(zos, "spring-components.json", springInfo);

      // Add Kubernetes info
      SystemInfoDtos.KubernetesInfo k8sInfo = systemInfoService.getKubernetesInfo();
      if (k8sInfo.isRunningInKubernetes()) {
        addToZip(zos, "kubernetes.json", k8sInfo);
      }

      // Add Storage info
      SystemInfoDtos.StorageInfo storageInfo = systemInfoService.getStorageInfo();
      addToZip(zos, "storage.json", storageInfo);

      // Add Kafka info
      SystemInfoDtos.KafkaInfo kafkaInfo = systemInfoService.getKafkaInfo();
      addToZip(zos, "kafka.json", kafkaInfo);

      // Add summary
      SystemInfoDtos.SystemInfo summary =
          SystemInfoDtos.SystemInfo.builder()
              .timestamp(LocalDateTime.now())
              .springComponents(springInfo)
              .kubernetesInfo(k8sInfo)
              .storageInfo(storageInfo)
              .kafkaInfo(kafkaInfo)
              .build();
      addToZip(zos, "summary.json", summary);

      zos.close();

      ByteArrayResource resource = new ByteArrayResource(baos.toByteArray());

      return ResponseEntity.ok()
          .header(
              HttpHeaders.CONTENT_DISPOSITION,
              "attachment; filename=datahub-system-info-" + System.currentTimeMillis() + ".zip")
          .contentType(MediaType.parseMediaType("application/zip"))
          .body(resource);

    } catch (Exception e) {
      return ResponseEntity.internalServerError().build();
    }
  }

  @Operation(
      summary = "Get Spring components information",
      description = "Retrieves Spring properties from GMS, MAE Consumer, and MCE Consumer")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved Spring components info",
            content =
                @Content(
                    schema = @Schema(implementation = SystemInfoDtos.SpringComponentsInfo.class)))
      })
  @GetMapping(value = "/spring", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.SpringComponentsInfo> getSpringComponentsInfo() {
    return ResponseEntity.ok(systemInfoService.getSpringComponentsInfo());
  }

  @Operation(
      summary = "Get specific Spring component information",
      description = "Retrieves Spring properties for a specific component")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved component info",
            content =
                @Content(schema = @Schema(implementation = SystemInfoDtos.ComponentInfo.class))),
        @ApiResponse(responseCode = "404", description = "Component not found")
      })
  @GetMapping(value = "/spring/{component}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.ComponentInfo> getSpringComponentInfo(
      @Parameter(description = "Component name (gms, mae-consumer, mce-consumer)") @PathVariable
          String component) {

    SystemInfoDtos.SpringComponentsInfo info = systemInfoService.getSpringComponentsInfo();
    SystemInfoDtos.ComponentInfo componentInfo = null;

    switch (component.toLowerCase()) {
      case "gms":
        componentInfo = info.getGms();
        break;
      case "mae-consumer":
        componentInfo = info.getMaeConsumer();
        break;
      case "mce-consumer":
        componentInfo = info.getMceConsumer();
        break;
    }

    if (componentInfo != null
        && componentInfo.getStatus() == SystemInfoDtos.ComponentStatus.AVAILABLE) {
      return ResponseEntity.ok(componentInfo);
    } else {
      return ResponseEntity.notFound().build();
    }
  }

  @Operation(
      summary = "Get Kubernetes information",
      description = "Retrieves Kubernetes deployment information if running in K8s")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved Kubernetes info",
            content =
                @Content(schema = @Schema(implementation = SystemInfoDtos.KubernetesInfo.class))),
        @ApiResponse(responseCode = "404", description = "Not running in Kubernetes")
      })
  @GetMapping(value = "/kubernetes", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.KubernetesInfo> getKubernetesInfo() {
    SystemInfoDtos.KubernetesInfo k8sInfo = systemInfoService.getKubernetesInfo();
    if (k8sInfo.isRunningInKubernetes()) {
      return ResponseEntity.ok(k8sInfo);
    } else {
      return ResponseEntity.notFound().build();
    }
  }

  @Operation(
      summary = "Get storage information",
      description =
          "Retrieves information about all storage technologies (MySQL, PostgreSQL, Elasticsearch, etc.)")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved storage info",
            content = @Content(schema = @Schema(implementation = SystemInfoDtos.StorageInfo.class)))
      })
  @GetMapping(value = "/storage", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.StorageInfo> getStorageInfo() {
    return ResponseEntity.ok(systemInfoService.getStorageInfo());
  }

  @Operation(
      summary = "Get specific storage information",
      description = "Retrieves information about a specific storage technology")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved storage info",
            content =
                @Content(
                    schema = @Schema(implementation = SystemInfoDtos.StorageSystemInfo.class))),
        @ApiResponse(
            responseCode = "404",
            description = "Storage system not found or not configured")
      })
  @GetMapping(value = "/storage/{type}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.StorageSystemInfo> getSpecificStorageInfo(
      @Parameter(
              description =
                  "Storage type (mysql, postgres, elasticsearch, opensearch, neo4j, cassandra)")
          @PathVariable
          String type) {

    SystemInfoDtos.StorageInfo storageInfo = systemInfoService.getStorageInfo();
    SystemInfoDtos.StorageSystemInfo systemInfo = null;

    switch (type.toLowerCase()) {
      case "mysql":
        systemInfo = storageInfo.getMysql();
        break;
      case "postgres":
        systemInfo = storageInfo.getPostgres();
        break;
      case "elasticsearch":
        systemInfo = storageInfo.getElasticsearch();
        break;
      case "opensearch":
        systemInfo = storageInfo.getOpensearch();
        break;
      case "neo4j":
        systemInfo = storageInfo.getNeo4j();
        break;
      case "cassandra":
        systemInfo = storageInfo.getCassandra();
        break;
    }

    if (systemInfo != null && systemInfo.isAvailable()) {
      return ResponseEntity.ok(systemInfo);
    } else {
      return ResponseEntity.notFound().build();
    }
  }

  @Operation(
      summary = "Get Kafka information",
      description =
          "Retrieves Kafka cluster information including brokers, topics, and schema registry")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved Kafka info",
            content = @Content(schema = @Schema(implementation = SystemInfoDtos.KafkaInfo.class)))
      })
  @GetMapping(value = "/kafka", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SystemInfoDtos.KafkaInfo> getKafkaInfo() {
    return ResponseEntity.ok(systemInfoService.getKafkaInfo());
  }

  private void addToZip(ZipOutputStream zos, String filename, Object data) throws Exception {
    ZipEntry entry = new ZipEntry(filename);
    zos.putNextEntry(entry);
    String json =
        systemOperationContext
            .getObjectMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(data);
    zos.write(json.getBytes());
    zos.closeEntry();
  }
}
