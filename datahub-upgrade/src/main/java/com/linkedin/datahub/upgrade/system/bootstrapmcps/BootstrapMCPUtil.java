package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.linkedin.common.AuditStamp;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Slf4j
public class BootstrapMCPUtil {
  static final MustacheFactory MUSTACHE_FACTORY = new DefaultMustacheFactory();

  private BootstrapMCPUtil() {}

  static List<UpgradeStep> generateSteps(
      @Nonnull OperationContext opContext,
      boolean isBlocking,
      @Nonnull String bootstrapMCPConfig,
      @Nonnull EntityService<?> entityService)
      throws IOException {
    List<UpgradeStep> steps =
        resolveYamlConf(opContext, bootstrapMCPConfig, BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .stream()
            .map(cfg -> cfg.withOverride(opContext.getObjectMapper()))
            .filter(cfg -> cfg.isBlocking() == isBlocking)
            .map(cfg -> new BootstrapMCPStep(opContext, entityService, cfg))
            .collect(Collectors.toList());

    log.info(
        "Generated {} {} BootstrapMCP steps",
        steps.size(),
        isBlocking ? "blocking" : "non-blocking");
    return steps;
  }

  static AspectsBatch generateAspectBatch(
      OperationContext opContext, BootstrapMCPConfigFile.MCPTemplate mcpTemplate)
      throws IOException {

    final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

    List<MetadataChangeProposal> mcps =
        resolveMCPTemplate(opContext, mcpTemplate, auditStamp).stream()
            .map(
                mcpObjectNode -> {
                  ObjectNode aspect = (ObjectNode) mcpObjectNode.remove("aspect");

                  MetadataChangeProposal mcp =
                      opContext
                          .getObjectMapper()
                          .convertValue(mcpObjectNode, MetadataChangeProposal.class);

                  try {
                    String jsonAspect =
                        opContext
                            .getObjectMapper()
                            .writeValueAsString(
                                convenienceConversions(opContext, mcp.getAspectName(), aspect));
                    GenericAspect genericAspect = GenericRecordUtils.serializeAspect(jsonAspect);
                    mcp.setAspect(genericAspect);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }

                  return mcp;
                })
            .collect(Collectors.toList());

    return AspectsBatchImpl.builder()
        .mcps(mcps, auditStamp, opContext.getRetrieverContext())
        .retrieverContext(opContext.getRetrieverContext())
        .build();
  }

  static List<ObjectNode> resolveMCPTemplate(
      OperationContext opContext,
      BootstrapMCPConfigFile.MCPTemplate mcpTemplate,
      AuditStamp auditStamp)
      throws IOException {

    final String template = loadTemplate(mcpTemplate.getMcps_location());
    Map<String, Object> scopeValues = resolveValues(opContext, mcpTemplate, auditStamp);

    StringWriter writer = new StringWriter();
    try {
      Mustache mustache =
          MUSTACHE_FACTORY.compile(new StringReader(template), mcpTemplate.getName());
      mustache.execute(writer, scopeValues);
    } catch (Exception e) {
      log.error(
          "Failed to apply mustache template. Template: {} Values: {}",
          template,
          resolveEnv(mcpTemplate));
      throw e;
    }

    final String yaml = writer.toString();
    try {
      return opContext.getYamlMapper().readValue(yaml, new TypeReference<>() {});
    } catch (Exception e) {
      log.error("Failed to parse rendered MCP bootstrap yaml: {}", yaml);
      throw e;
    }
  }

  static Map<String, Object> resolveValues(
      OperationContext opContext,
      BootstrapMCPConfigFile.MCPTemplate mcpTemplate,
      AuditStamp auditStamp)
      throws IOException {
    final Map<String, Object> scopeValues = new HashMap<>();

    // built-in
    scopeValues.put("auditStamp", RecordUtils.toJsonString(auditStamp));

    String envValue = resolveEnv(mcpTemplate);
    if (envValue != null) {
      scopeValues.putAll(opContext.getObjectMapper().readValue(envValue, new TypeReference<>() {}));
    }
    return scopeValues;
  }

  @Nullable
  private static String resolveEnv(BootstrapMCPConfigFile.MCPTemplate mcpTemplate) {
    if (mcpTemplate.getValues_env() != null
        && !mcpTemplate.getValues_env().isEmpty()
        && System.getenv().containsKey(mcpTemplate.getValues_env())) {
      return System.getenv(mcpTemplate.getValues_env());
    }
    return null;
  }

  private static String loadTemplate(String source) throws IOException {
    log.info("Loading MCP template {}", source);
    try (InputStream stream = new ClassPathResource(source).getInputStream()) {
      log.info("Found in classpath: {}", source);
      return IOUtils.toString(stream, StandardCharsets.UTF_8);
    } catch (FileNotFoundException e) {
      log.info("{} was NOT found in the classpath.", source);
      try (InputStream stream = new FileSystemResource(source).getInputStream()) {
        log.info("Found in filesystem: {}", source);
        return IOUtils.toString(stream, StandardCharsets.UTF_8);
      } catch (Exception e2) {
        throw new IllegalArgumentException(String.format("Could not resolve %s", source));
      }
    }
  }

  static <T> T resolveYamlConf(OperationContext opContext, String source, Class<T> clazz)
      throws IOException {
    log.info("Resolving {} to {}", source, clazz.getSimpleName());
    try (InputStream stream = new ClassPathResource(source).getInputStream()) {
      log.info("Found in classpath: {}", source);
      return opContext.getYamlMapper().readValue(stream, clazz);
    } catch (FileNotFoundException e) {
      log.info("{} was NOT found in the classpath.", source);
      try (InputStream stream = new FileSystemResource(source).getInputStream()) {
        log.info("Found in filesystem: {}", source);
        return opContext.getYamlMapper().readValue(stream, clazz);
      } catch (Exception e2) {
        throw new IllegalArgumentException(String.format("Could not resolve %s", source));
      }
    }
  }

  private static ObjectNode convenienceConversions(
      OperationContext opContext, String aspectName, ObjectNode aspectObjectNode)
      throws JsonProcessingException {
    if (INGESTION_INFO_ASPECT_NAME.equals(aspectName)) {
      ObjectNode config = (ObjectNode) aspectObjectNode.get("config");
      ObjectNode recipe = (ObjectNode) config.remove("recipe");
      config.put("recipe", opContext.getObjectMapper().writeValueAsString(recipe));
    }
    return aspectObjectNode;
  }
}
