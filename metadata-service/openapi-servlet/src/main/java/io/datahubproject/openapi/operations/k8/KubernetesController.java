package io.datahubproject.openapi.operations.k8;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.request.RequestContext;
import io.datahubproject.openapi.operations.k8.models.ConfigMapUpdateRequest;
import io.datahubproject.openapi.operations.k8.models.CronJobTriggerRequest;
import io.datahubproject.openapi.operations.k8.models.DeploymentEnvUpdateRequest;
import io.datahubproject.openapi.operations.k8.models.DeploymentScaleRequest;
import io.datahubproject.openapi.operations.k8.models.K8sStatusResponse;
import io.datahubproject.openapi.v1.models.registry.PaginatedResponse;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for Kubernetes operations API.
 *
 * <p><b>WARNING:</b> This API allows direct modification of Kubernetes resources and should be used
 * with extreme caution in production environments. Operations like scaling deployments, modifying
 * environment variables, updating ConfigMaps, and deleting pods/jobs can cause service disruptions.
 * Ensure proper RBAC controls and audit logging are in place before enabling in production.
 */
@ConditionalOnProperty(
    name = "kubernetes.operationsApiEnabled",
    havingValue = "true",
    matchIfMissing = true)
@RestController
@RequestMapping("/openapi/operations/k8")
@Slf4j
@Tag(
    name = "Kubernetes Operations",
    description =
        "APIs for managing Kubernetes resources in the current namespace. "
            + "⚠️ WARNING: These APIs can modify cluster resources and should be used with caution "
            + "in production environments. Requires MANAGE_SYSTEM_OPERATIONS privilege.")
public class KubernetesController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final KubernetesClient kubernetesClient;
  private final ObjectMapper k8sObjectMapper;

  @Autowired
  public KubernetesController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      @Autowired(required = false) KubernetesClient kubernetesClient,
      @Qualifier("kubernetesObjectMapper") ObjectMapper k8sObjectMapper) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.kubernetesClient = kubernetesClient;
    this.k8sObjectMapper = k8sObjectMapper;
  }

  // ==================== Status ====================

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get Kubernetes integration status",
      description = "Check if Kubernetes integration is available and get namespace information",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Status retrieved successfully",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = K8sStatusResponse.class))),
        @ApiResponse(responseCode = "403", description = "Not authorized")
      })
  public ResponseEntity<?> getStatus(HttpServletRequest request) {
    // Auth check only (status endpoint should work even when K8s unavailable)
    Authentication auth = AuthenticationContext.getAuthentication();
    String actor = auth.getActor().toUrnStr();
    OperationContext ctx =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actor, request, "getK8sStatus", List.of()),
            authorizerChain,
            auth,
            true);
    if (!AuthUtil.isAPIAuthorized(ctx, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(Map.of("error", actor + " is not authorized for Kubernetes operations"));
    }

    if (kubernetesClient == null) {
      return ResponseEntity.ok(
          K8sStatusResponse.builder()
              .available(false)
              .reason("Not running in Kubernetes environment")
              .build());
    }
    return ResponseEntity.ok(K8sStatusResponse.builder().available(true).namespace(ns()).build());
  }

  // ==================== Deployments ====================

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/deployments", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List deployments",
      description = "List all deployments in the current namespace",
      responses = {
        @ApiResponse(responseCode = "200", description = "Deployments listed successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> listDeployments(
      HttpServletRequest request,
      @RequestParam(name = "start", defaultValue = "0") Integer start,
      @RequestParam(name = "count", defaultValue = "10") Integer count) {
    return withAccess(
        request,
        "listDeployments",
        () -> {
          var items = kubernetesClient.apps().deployments().inNamespace(ns()).list().getItems();
          return k8sResponse(paginate(items, start, count));
        });
  }

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/deployments/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get deployment",
      description = "Get details of a specific deployment",
      responses = {
        @ApiResponse(responseCode = "200", description = "Deployment retrieved successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Deployment not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> getDeployment(
      HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "getDeployment",
        () -> {
          var d = kubernetesClient.apps().deployments().inNamespace(ns()).withName(name).get();
          return d != null ? k8sResponse(d) : notFound("Deployment", name);
        });
  }

  @Tag(name = "Kubernetes Operations")
  @PatchMapping(path = "/deployments/{name}/scale", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Scale deployment",
      description =
          "Scale replicas and/or update resource limits/requests for a deployment. "
              + "⚠️ WARNING: This can cause service disruptions.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Deployment scaled successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Deployment not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> scaleDeployment(
      HttpServletRequest request,
      @PathVariable("name") String name,
      @RequestBody DeploymentScaleRequest req) {
    return withAccess(
        request,
        "scaleDeployment",
        () -> {
          if (req.getReplicas() == null && req.getResources() == null) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "At least one of replicas or resources must be provided"));
          }
          if (req.getReplicas() != null && req.getReplicas() < 0) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "Replicas must be non-negative"));
          }

          var deployments = kubernetesClient.apps().deployments().inNamespace(ns());
          var d = deployments.withName(name).get();
          if (d == null) return notFound("Deployment", name);

          if (req.getReplicas() != null) {
            deployments.withName(name).scale(req.getReplicas());
            log.info("Scaled deployment {} to {} replicas", name, req.getReplicas());
          }

          if (req.getResources() != null) {
            d = deployments.withName(name).get(); // Refetch after scale
            var cr =
                resolveContainer(
                    d.getSpec().getTemplate().getSpec().getContainers(),
                    req.getContainerName(),
                    name);
            if (cr.error() != null) return cr.error();

            String containerName = cr.name();
            deployments
                .withName(name)
                .edit(
                    dep -> {
                      dep.getSpec().getTemplate().getSpec().getContainers().stream()
                          .filter(c -> c.getName().equals(containerName))
                          .forEach(c -> c.setResources(req.getResources()));
                      return dep;
                    });
            log.info("Updated resources for deployment {}, container {}", name, containerName);
          }

          return k8sResponse(deployments.withName(name).get());
        });
  }

  @Tag(name = "Kubernetes Operations")
  @PatchMapping(path = "/deployments/{name}/env", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Update deployment environment variables",
      description =
          "Add, update, or remove environment variables for a container in a deployment. "
              + "⚠️ WARNING: This triggers a rolling restart and can cause service disruptions.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Environment variables updated"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Deployment or container not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> updateDeploymentEnv(
      HttpServletRequest request,
      @PathVariable("name") String name,
      @RequestBody DeploymentEnvUpdateRequest req) {
    return withAccess(
        request,
        "updateDeploymentEnv",
        () -> {
          boolean hasSet = req.getSet() != null && !req.getSet().isEmpty();
          boolean hasRemove = req.getRemove() != null && !req.getRemove().isEmpty();
          if (!hasSet && !hasRemove) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "At least one of 'set' or 'remove' must be provided"));
          }

          var deployments = kubernetesClient.apps().deployments().inNamespace(ns());
          var d = deployments.withName(name).get();
          if (d == null) return notFound("Deployment", name);

          var cr =
              resolveContainer(
                  d.getSpec().getTemplate().getSpec().getContainers(),
                  req.getContainerName(),
                  name);
          if (cr.error() != null) return cr.error();

          String containerName = cr.name();
          deployments
              .withName(name)
              .edit(
                  dep -> {
                    dep.getSpec().getTemplate().getSpec().getContainers().stream()
                        .filter(c -> c.getName().equals(containerName))
                        .forEach(
                            c -> {
                              List<EnvVar> envVars =
                                  c.getEnv() != null
                                      ? new ArrayList<>(c.getEnv())
                                      : new ArrayList<>();
                              if (req.getRemove() != null) {
                                envVars.removeIf(e -> req.getRemove().contains(e.getName()));
                              }
                              if (req.getSet() != null) {
                                req.getSet()
                                    .forEach(
                                        (k, v) -> {
                                          envVars.removeIf(e -> e.getName().equals(k));
                                          envVars.add(new EnvVar(k, v, null));
                                        });
                              }
                              c.setEnv(envVars);
                            });
                    return dep;
                  });

          log.info("Updated env vars for deployment {}, container {}", name, containerName);
          return k8sResponse(deployments.withName(name).get());
        });
  }

  // ==================== Pods ====================

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/pods", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List pods",
      description = "List all pods in the current namespace",
      responses = {
        @ApiResponse(responseCode = "200", description = "Pods listed successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> listPods(
      HttpServletRequest request,
      @RequestParam(name = "start", defaultValue = "0") Integer start,
      @RequestParam(name = "count", defaultValue = "10") Integer count) {
    return withAccess(
        request,
        "listPods",
        () -> {
          var items = kubernetesClient.pods().inNamespace(ns()).list().getItems();
          return k8sResponse(paginate(items, start, count));
        });
  }

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/pods/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get pod",
      description = "Get details of a specific pod",
      responses = {
        @ApiResponse(responseCode = "200", description = "Pod retrieved successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Pod not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> getPod(HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "getPod",
        () -> {
          var pod = kubernetesClient.pods().inNamespace(ns()).withName(name).get();
          return pod != null ? k8sResponse(pod) : notFound("Pod", name);
        });
  }

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/pods/{name}/logs", produces = MediaType.TEXT_PLAIN_VALUE)
  @Operation(
      summary = "Get pod logs",
      description = "Get recent logs from a pod container, truncated to limitBytes",
      responses = {
        @ApiResponse(responseCode = "200", description = "Logs retrieved successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Pod or container not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> getPodLogs(
      HttpServletRequest request,
      @PathVariable("name") String name,
      @RequestParam(name = "container", required = false) String container,
      @RequestParam(name = "sinceSeconds", required = false) Integer sinceSeconds,
      @RequestParam(name = "limitBytes", defaultValue = "524288") Integer limitBytes) {
    return withAccess(
        request,
        "getPodLogs",
        () -> {
          int limit = Math.min(Math.max(limitBytes, 1024), 1024 * 1024); // 1KB min, 1MB max

          var pods = kubernetesClient.pods().inNamespace(ns());
          var pod = pods.withName(name).get();
          if (pod == null)
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Pod not found: " + name);

          var cr = resolveContainer(pod.getSpec().getContainers(), container, name);
          if (cr.error() != null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(cr.error().getBody().toString());
          }

          try {
            var logResource = pods.withName(name).inContainer(cr.name());
            String logs =
                (sinceSeconds != null && sinceSeconds > 0)
                    ? logResource.sinceSeconds(sinceSeconds).getLog()
                    : logResource.getLog();
            if (logs != null && logs.length() > limit) {
              logs = "[truncated...]\n" + logs.substring(logs.length() - limit);
            }
            return ResponseEntity.ok(logs != null ? logs : "");
          } catch (Exception e) {
            log.error("Failed to get logs for pod {}, container {}", name, cr.name(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Failed to retrieve logs: " + e.getMessage());
          }
        });
  }

  @Tag(name = "Kubernetes Operations")
  @DeleteMapping(path = "/pods/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Delete pod",
      description =
          "Delete a pod (typically to trigger a restart via deployment). "
              + "⚠️ WARNING: This immediately terminates the pod.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Pod deleted successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Pod not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> deletePod(
      HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "deletePod",
        () -> {
          var pods = kubernetesClient.pods().inNamespace(ns());
          if (pods.withName(name).get() == null) return notFound("Pod", name);
          pods.withName(name).delete();
          log.info("Deleted pod {}", name);
          return ResponseEntity.ok(Map.of("message", "Pod deleted: " + name));
        });
  }

  // ==================== ConfigMaps ====================

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/configmaps", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List ConfigMaps",
      description = "List all ConfigMaps in the current namespace",
      responses = {
        @ApiResponse(responseCode = "200", description = "ConfigMaps listed successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> listConfigMaps(
      HttpServletRequest request,
      @RequestParam(name = "start", defaultValue = "0") Integer start,
      @RequestParam(name = "count", defaultValue = "10") Integer count) {
    return withAccess(
        request,
        "listConfigMaps",
        () -> {
          var items = kubernetesClient.configMaps().inNamespace(ns()).list().getItems();
          return k8sResponse(paginate(items, start, count));
        });
  }

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/configmaps/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get ConfigMap",
      description = "Get details of a specific ConfigMap",
      responses = {
        @ApiResponse(responseCode = "200", description = "ConfigMap retrieved successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "ConfigMap not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> getConfigMap(
      HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "getConfigMap",
        () -> {
          var cm = kubernetesClient.configMaps().inNamespace(ns()).withName(name).get();
          return cm != null ? k8sResponse(cm) : notFound("ConfigMap", name);
        });
  }

  @Tag(name = "Kubernetes Operations")
  @PatchMapping(path = "/configmaps/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Update ConfigMap",
      description =
          "Add, update, or remove data entries in a ConfigMap. "
              + "⚠️ WARNING: Pods using this ConfigMap may need restart to pick up changes.",
      responses = {
        @ApiResponse(responseCode = "200", description = "ConfigMap updated successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "ConfigMap not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> updateConfigMap(
      HttpServletRequest request,
      @PathVariable("name") String name,
      @RequestBody ConfigMapUpdateRequest req) {
    return withAccess(
        request,
        "updateConfigMap",
        () -> {
          boolean hasSet = req.getSet() != null && !req.getSet().isEmpty();
          boolean hasRemove = req.getRemove() != null && !req.getRemove().isEmpty();
          if (!hasSet && !hasRemove) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "At least one of 'set' or 'remove' must be provided"));
          }

          var configMaps = kubernetesClient.configMaps().inNamespace(ns());
          var cm = configMaps.withName(name).get();
          if (cm == null) return notFound("ConfigMap", name);

          configMaps
              .withName(name)
              .edit(
                  c -> {
                    var data =
                        c.getData() != null ? c.getData() : new java.util.HashMap<String, String>();
                    if (req.getRemove() != null) {
                      req.getRemove().forEach(data::remove);
                    }
                    if (req.getSet() != null) {
                      data.putAll(req.getSet());
                    }
                    c.setData(data);
                    return c;
                  });

          log.info("Updated ConfigMap {}", name);
          return k8sResponse(configMaps.withName(name).get());
        });
  }

  // ==================== CronJobs ====================

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/cronjobs", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List CronJobs",
      description = "List all CronJobs in the current namespace",
      responses = {
        @ApiResponse(responseCode = "200", description = "CronJobs listed successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> listCronJobs(
      HttpServletRequest request,
      @RequestParam(name = "start", defaultValue = "0") Integer start,
      @RequestParam(name = "count", defaultValue = "10") Integer count) {
    return withAccess(
        request,
        "listCronJobs",
        () -> {
          var items = kubernetesClient.batch().v1().cronjobs().inNamespace(ns()).list().getItems();
          return k8sResponse(paginate(items, start, count));
        });
  }

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/cronjobs/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get CronJob",
      description = "Get details of a specific CronJob",
      responses = {
        @ApiResponse(responseCode = "200", description = "CronJob retrieved successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "CronJob not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> getCronJob(
      HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "getCronJob",
        () -> {
          var cj = kubernetesClient.batch().v1().cronjobs().inNamespace(ns()).withName(name).get();
          return cj != null ? k8sResponse(cj) : notFound("CronJob", name);
        });
  }

  @Tag(name = "Kubernetes Operations")
  @PostMapping(path = "/cronjobs/{name}/trigger", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Trigger CronJob",
      description =
          "Manually trigger a CronJob by creating a Job with optional overrides. "
              + "⚠️ WARNING: Ensure the job is safe to run outside its scheduled time.",
      responses = {
        @ApiResponse(responseCode = "201", description = "Job created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "CronJob not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> triggerCronJob(
      HttpServletRequest request,
      @PathVariable("name") String name,
      @RequestBody CronJobTriggerRequest req) {
    return withAccess(
        request,
        "triggerCronJob",
        () -> {
          if (req.getJobName() == null || req.getJobName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Job name is required"));
          }

          var cronJob =
              kubernetesClient.batch().v1().cronjobs().inNamespace(ns()).withName(name).get();
          if (cronJob == null) return notFound("CronJob", name);

          Job job =
              new JobBuilder()
                  .withNewMetadata()
                  .withName(req.getJobName())
                  .withNamespace(ns())
                  .addToLabels("app.kubernetes.io/created-by", "datahub-k8s-controller")
                  .addToLabels("app.kubernetes.io/triggered-from", name)
                  .endMetadata()
                  .withSpec(cronJob.getSpec().getJobTemplate().getSpec())
                  .build();

          if (hasOverrides(req)) {
            var containers = job.getSpec().getTemplate().getSpec().getContainers();
            var cr = resolveContainer(containers, req.getContainerName(), name);
            if (cr.error() != null) return cr.error();

            containers.stream()
                .filter(c -> c.getName().equals(cr.name()))
                .findFirst()
                .ifPresent(
                    c -> {
                      if (req.getCommand() != null && !req.getCommand().isEmpty())
                        c.setCommand(req.getCommand());
                      if (req.getArgs() != null && !req.getArgs().isEmpty())
                        c.setArgs(req.getArgs());
                      if (req.getResources() != null) c.setResources(req.getResources());
                    });
          }

          var created = kubernetesClient.batch().v1().jobs().inNamespace(ns()).create(job);
          log.info("Created job {} from CronJob {}", req.getJobName(), name);
          return k8sResponse(created, HttpStatus.CREATED);
        });
  }

  // ==================== Jobs ====================

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/jobs", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List Jobs",
      description = "List all Jobs in the current namespace",
      responses = {
        @ApiResponse(responseCode = "200", description = "Jobs listed successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> listJobs(
      HttpServletRequest request,
      @RequestParam(name = "start", defaultValue = "0") Integer start,
      @RequestParam(name = "count", defaultValue = "10") Integer count) {
    return withAccess(
        request,
        "listJobs",
        () -> {
          var items = kubernetesClient.batch().v1().jobs().inNamespace(ns()).list().getItems();
          return k8sResponse(paginate(items, start, count));
        });
  }

  @Tag(name = "Kubernetes Operations")
  @GetMapping(path = "/jobs/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get Job",
      description = "Get details of a specific Job",
      responses = {
        @ApiResponse(responseCode = "200", description = "Job retrieved successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Job not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> getJob(HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "getJob",
        () -> {
          var job = kubernetesClient.batch().v1().jobs().inNamespace(ns()).withName(name).get();
          return job != null ? k8sResponse(job) : notFound("Job", name);
        });
  }

  @Tag(name = "Kubernetes Operations")
  @DeleteMapping(path = "/jobs/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Delete Job",
      description =
          "Delete a Job and its associated pods. "
              + "⚠️ WARNING: This immediately terminates any running pods for this job.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Job deleted successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized"),
        @ApiResponse(responseCode = "404", description = "Job not found"),
        @ApiResponse(responseCode = "503", description = "Kubernetes not available")
      })
  public ResponseEntity<?> deleteJob(
      HttpServletRequest request, @PathVariable("name") String name) {
    return withAccess(
        request,
        "deleteJob",
        () -> {
          var jobs = kubernetesClient.batch().v1().jobs().inNamespace(ns());
          if (jobs.withName(name).get() == null) return notFound("Job", name);
          jobs.withName(name).delete();
          log.info("Deleted job {}", name);
          return ResponseEntity.ok(Map.of("message", "Job deleted: " + name));
        });
  }

  // ==================== Helper Methods ====================

  /** Runs operation after auth and K8s availability checks. Returns error response or null. */
  private ResponseEntity<?> checkAccess(HttpServletRequest request, String operation) {
    Authentication auth = AuthenticationContext.getAuthentication();
    String actor = auth.getActor().toUrnStr();
    OperationContext ctx =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actor, request, operation, List.of()),
            authorizerChain,
            auth,
            true);

    if (!AuthUtil.isAPIAuthorized(ctx, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(Map.of("error", actor + " is not authorized for Kubernetes operations"));
    }
    if (kubernetesClient == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("error", "Kubernetes integration not available"));
    }
    return null;
  }

  /** Executes operation if authorized and K8s available, otherwise returns error. */
  private ResponseEntity<?> withAccess(
      HttpServletRequest request, String operation, Supplier<ResponseEntity<?>> handler) {
    ResponseEntity<?> error = checkAccess(request, operation);
    return error != null ? error : handler.get();
  }

  private String ns() {
    return kubernetesClient.getConfiguration().getNamespace();
  }

  private ResponseEntity<?> notFound(String type, String name) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
        .body(Map.of("error", type + " not found: " + name));
  }

  /** Resolves container name, defaulting to first if blank. Returns error response or null. */
  private record ContainerResult(String name, ResponseEntity<?> error) {}

  private ContainerResult resolveContainer(
      List<Container> containers, String requested, String parentName) {
    if (containers.isEmpty()) {
      return new ContainerResult(null, notFound("Containers in", parentName));
    }
    String name =
        (requested != null && !requested.isBlank()) ? requested : containers.get(0).getName();
    if (containers.stream().noneMatch(c -> c.getName().equals(name))) {
      return new ContainerResult(null, notFound("Container", name));
    }
    return new ContainerResult(name, null);
  }

  private boolean hasOverrides(CronJobTriggerRequest req) {
    return (req.getCommand() != null && !req.getCommand().isEmpty())
        || (req.getArgs() != null && !req.getArgs().isEmpty())
        || req.getResources() != null;
  }

  private <T> PaginatedResponse<T> paginate(List<T> items, int start, int count) {
    int total = items.size();
    int s = Math.min(start, total);
    int e = Math.min(s + count, total);
    List<T> page = items.subList(s, e);
    return PaginatedResponse.<T>builder()
        .elements(page)
        .start(s)
        .count(page.size())
        .total(total)
        .build();
  }

  /** Serialize K8s object using the scoped ObjectMapper that strips verbose metadata. */
  private ResponseEntity<String> k8sResponse(Object k8sObject) {
    try {
      String json = k8sObjectMapper.writeValueAsString(k8sObject);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize K8s object", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("{\"error\": \"Serialization failed\"}");
    }
  }

  /** Serialize K8s object with custom status code. */
  private ResponseEntity<String> k8sResponse(Object k8sObject, HttpStatus status) {
    try {
      String json = k8sObjectMapper.writeValueAsString(k8sObject);
      return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize K8s object", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("{\"error\": \"Serialization failed\"}");
    }
  }
}
