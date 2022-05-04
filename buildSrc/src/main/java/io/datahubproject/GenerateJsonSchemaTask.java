package io.datahubproject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.fge.jackson.JacksonUtils;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import static com.github.fge.processing.ProcessingUtil.*;
import static org.apache.commons.io.FilenameUtils.*;


@CacheableTask
public class GenerateJsonSchemaTask extends DefaultTask {
  private String inputDirectory;
  private String outputDirectory;
  private ArrayNode aspectType;
  private Path combinedDirectory;
  private Path jsonDirectory;
  public static final String sep = FileSystems.getDefault().getSeparator();

  private static final JsonNodeFactory NODE_FACTORY = JacksonUtils.nodeFactory();

  public void setInputDirectory(String inputDirectory) {
    this.inputDirectory = inputDirectory;
  }

  @InputDirectory
  public String getInputDirectory() {
   return inputDirectory;
  }

  @OutputDirectory
  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }

  @TaskAction
  public void generate() throws IOException {
    Path baseDir = Paths.get(inputDirectory);
    aspectType = NODE_FACTORY.arrayNode();

    Path schemaDirectory = Paths.get(outputDirectory);
    jsonDirectory = Paths.get(schemaDirectory + sep + "json");
    try {
      Files.createDirectory(schemaDirectory);
    } catch (FileAlreadyExistsException fae) {
      // No-op
    }
    try {
      Files.createDirectory(jsonDirectory);
    } catch (FileAlreadyExistsException fae) {
      // No-op
    }
    Files.walk(baseDir)
        .filter(Files::isRegularFile)
        .map(Path::toFile)
        .forEach(this::generateSchema);
    List<ObjectNode> nodesList = Files.walk(jsonDirectory)
        .filter(Files::isRegularFile)
        .filter(path -> {
          String fileName = path.toFile().getName();
          return !getBaseName(fileName).contains("ChangeEvent") && !getBaseName(fileName).contains("AuditEvent");
        })
        .map(Path::toFile)
        .map(file -> {
          try {
            return (ObjectNode) JsonLoader.fromFile(file);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());

    ObjectNode schemasNode = NODE_FACTORY.objectNode();
    nodesList.forEach(objectNode -> {
      ObjectNode definitions = (ObjectNode) objectNode.get("definitions");
      // Modify GenericAspect and EnvelopedAspect to have all Aspect subtypes
      if (definitions.has("GenericAspect")) {
        ObjectNode genericAspect = (ObjectNode) definitions.get("GenericAspect");
        ((ObjectNode) genericAspect.get("properties")).replace("value", NODE_FACTORY.objectNode().set("oneOf", aspectType));
      }
      if (definitions.has("EnvelopedAspect")) {
        ObjectNode envelopedAspect = (ObjectNode) definitions.get("EnvelopedAspect");
        ((ObjectNode) envelopedAspect.get("properties")).replace("value", NODE_FACTORY.objectNode().set("oneOf", aspectType));
      }
      schemasNode.setAll(definitions);
    });
    /*
    Minimal OpenAPI header
    openapi: 3.0.1
    info:
      title: OpenAPI definition
      version: v0
    servers:
      - url: http://localhost:8080/openapi
        description: Generated server url
    paths:
      /path:
        get:
          tags:
            - path
    */
    ObjectNode yamlHeader = (ObjectNode) ((ObjectNode) NODE_FACTORY.objectNode()
        .put("openapi", "3.0.1")
        .set("info", NODE_FACTORY.objectNode()
            .put("title", "OpenAPI Definition")
            .put("version", "v0")))
        .set("paths", NODE_FACTORY.objectNode()
            .set("/path", NODE_FACTORY.objectNode()
                .set("get", NODE_FACTORY.objectNode().set("tags", NODE_FACTORY.arrayNode().add("path")))));
    JsonNode combinedSchemaDefinitionsYaml = ((ObjectNode) NODE_FACTORY.objectNode().set("components",
        NODE_FACTORY.objectNode().set("schemas", schemasNode))).setAll(yamlHeader);

    final String yaml = new YAMLMapper().writeValueAsString(combinedSchemaDefinitionsYaml)
        .replaceAll("definitions", "components/schemas")
        .replaceAll("\n\\s+- type: \"null\"", "");

    combinedDirectory = Paths.get(outputDirectory + sep + "combined");
    try {
      Files.createDirectory(combinedDirectory);
    } catch (FileAlreadyExistsException fae) {
      // No-op
    }
    Files.write(Paths.get(combinedDirectory + sep + "open-api.yaml"),
        yaml.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);

    JsonNode combinedSchemaDefinitionsJson = NODE_FACTORY.objectNode().set("definitions",schemasNode);
    String prettySchema = JacksonUtils.prettyPrint(combinedSchemaDefinitionsJson);
    Files.write(Paths.get(Paths.get(outputDirectory) + sep + "combined" + sep + "schema.json"),
        prettySchema.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);

  }

  private final HashSet<String> filenames = new HashSet<>();
  private void generateSchema(final File file) {
    final String fileBaseName;
    try {
      final JsonNode schema = JsonLoader.fromFile(file);
      final JsonNode result = buildResult(schema.toString());
      String prettySchema = JacksonUtils.prettyPrint(result);
      Path absolutePath = file.getAbsoluteFile().toPath();
      if (absolutePath.endsWith(Paths.get("com", "linkedin", "metadata", "aspect", "EnvelopedAspect.avsc"))) {
        fileBaseName = "EnvelopedTimeseriesAspect";
        prettySchema = prettySchema.replaceAll("EnvelopedAspect", "EnvelopedTimeseriesAspect");
      } else if (!filenames.add(file.getName())) {
        System.out.println("Not processing legacy schema " + absolutePath);
        return;
      } else {
        fileBaseName = getBaseName(file.getName());
      }
      Files.write(Paths.get(jsonDirectory + sep + fileBaseName + ".json"),
          prettySchema.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
      if (schema.has("Aspect")) {
        aspectType.add(NODE_FACTORY.objectNode().put("$ref", "#/definitions/" + getBaseName(file.getName())));
      }
    } catch (IOException | ProcessingException e) {
      throw new RuntimeException(e);
    }
  }

}