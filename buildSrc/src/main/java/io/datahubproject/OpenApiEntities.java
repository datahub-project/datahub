package io.datahubproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.models.registry.config.Entities;
import com.linkedin.metadata.models.registry.config.Entity;
import org.gradle.internal.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class OpenApiEntities {
    private final static String MODEL_VERSION = "_v2";
    private final static String REQUEST_SUFFIX = "Request" + MODEL_VERSION;
    private final static String RESPONSE_SUFFIX = "Response" + MODEL_VERSION;

    private final static String ASPECT_REQUEST_SUFFIX = "Aspect" + REQUEST_SUFFIX;
    private final static String ASPECT_RESPONSE_SUFFIX = "Aspect" + RESPONSE_SUFFIX;
    private final static String ENTITY_REQUEST_SUFFIX = "Entity" + REQUEST_SUFFIX;
    private final static String ENTITY_RESPONSE_SUFFIX = "Entity" + RESPONSE_SUFFIX;

    private final JsonNodeFactory NODE_FACTORY;
    private Map<String, Entity> entityMap;
    private String entityRegistryYaml;
    private Path combinedDirectory;

    private final static ImmutableSet<Object> SUPPORTED_ASPECT_PATHS = ImmutableSet.builder()
                .add("domains")
                .add("ownership")
                .add("deprecation")
                .add("status")
                .add("globalTags")
                .add("glossaryTerms")
                .add("dataContractInfo")
                .add("browsePathsV2")
                .add("datasetProperties").add("editableDatasetProperties")
                .add("chartInfo").add("editableChartProperties")
                .add("dashboardInfo").add("editableDashboardProperties")
                .add("notebookInfo").add("editableNotebookProperties")
                .add("dataProductProperties")
                .add("institutionalMemory")
                .add("forms").add("formInfo").add("dynamicFormAssignment")
                .add("businessAttributeInfo")
                .build();

    private final static ImmutableSet<String> ENTITY_EXCLUSIONS = ImmutableSet.<String>builder()
            .add("structuredProperty")
            .build();

    public OpenApiEntities(JsonNodeFactory NODE_FACTORY) {
        this.NODE_FACTORY = NODE_FACTORY;
    }

    public String getEntityRegistryYaml() {
        return entityRegistryYaml;
    }

    public void setEntityRegistryYaml(String entityRegistryYaml) {
        this.entityRegistryYaml = entityRegistryYaml;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        try {
            Entities entities = mapper.readValue(Paths.get(entityRegistryYaml).toFile(), Entities.class);
            entityMap = entities.getEntities().stream()
                    .filter(e -> "core".equals(e.getCategory()))
                    .collect(Collectors.toMap(Entity::getName, Function.identity()));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Error while reading entity yaml file in path %s: %s", entityRegistryYaml, e.getMessage()));
        }
    }

    public Path getCombinedDirectory() {
        return combinedDirectory;
    }

    public void setCombinedDirectory(Path combinedDirectory) {
        this.combinedDirectory = combinedDirectory;
    }

    public ObjectNode entityExtension(List<ObjectNode> nodesList, ObjectNode schemasNode) throws IOException {
        // Generate entities schema
        Set<String> aspectDefinitions = nodesList.stream()
                .map(nl -> nl.get("definitions").fieldNames())
                .flatMap(it -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false))
                .collect(Collectors.toSet());
        withWrappedAspects(schemasNode, aspectDefinitions);

        // Add entity schema
        Set<String> entitySchema = withEntitySchema(schemasNode, aspectDefinitions);

        // Write specific sections: components.* and paths
        Set<String> modelDefinitions = Stream.concat(aspectDefinitions.stream(), entitySchema.stream())
                        .collect(Collectors.toSet());

        // Just the component & parameters schema
        Pair<ObjectNode, Set<String>> parameters = buildParameters(schemasNode, modelDefinitions);
        ObjectNode componentsNode = writeComponentsYaml(schemasNode, parameters.left());

        // Just the entity paths
        writePathsYaml(modelDefinitions, parameters.right());

        return componentsNode;
    }

    /**
     * Convert the pdl model names to desired class names. Upper case first letter unless the 3rd character is upper case.
     * i.e. mlModel -> MLModel
     *      dataset -> Dataset
     *      dataProduct -> DataProduct
     * @param s input string
     * @return class name
     */
    public static String toUpperFirst(String s) {
        if (s.length() > 2 && s.substring(2, 3).equals(s.substring(2, 3).toUpperCase())) {
            return s.substring(0, 2).toUpperCase() + s.substring(2);
        } else {
            return s.substring(0, 1).toUpperCase() + s.substring(1);
        }
    }

    private Set<String> withEntitySchema(ObjectNode schemasNode, Set<String> definitions) {
        return entityMap.values().stream()
                // Make sure the primary key is defined
                .filter(entity -> definitions.contains(toUpperFirst(entity.getKeyAspect())))
                .filter(entity -> !ENTITY_EXCLUSIONS.contains(entity.getName()))
                .map(entity -> {
                    final String upperName = toUpperFirst(entity.getName());

                    ObjectNode entityDefinitions = NODE_FACTORY.objectNode();
                    entityDefinitions.set(upperName + ENTITY_RESPONSE_SUFFIX, buildEntitySchema(entity, definitions, true));
                    entityDefinitions.set(upperName + ENTITY_REQUEST_SUFFIX, buildEntitySchema(entity, definitions, false));
                    entityDefinitions.set("Scroll" + upperName + ENTITY_RESPONSE_SUFFIX, buildEntityScrollSchema(entity));

                    schemasNode.setAll(entityDefinitions);

                    return upperName;
                }).collect(Collectors.toSet());
    }


    private Set<String> withWrappedAspects(ObjectNode schemasNode, Set<String> aspects) {
        return aspects.stream().peek(aspect -> {
            ObjectNode aspectRef = NODE_FACTORY.objectNode()
                    .put("$ref", "#/definitions/" + aspect);

            ObjectNode responseProperties = NODE_FACTORY.objectNode();
            responseProperties.set("value", aspectRef);
            responseProperties.set("systemMetadata", NODE_FACTORY.objectNode()
                    .put("description", "System metadata for the aspect.")
                    .put("$ref", "#/definitions/SystemMetadata"));

            ObjectNode responseWrapper = NODE_FACTORY.objectNode()
                    .put("type", "object")
                    .put("description", "Aspect wrapper object.")
                    .set("properties", responseProperties);
            responseWrapper.set("required", NODE_FACTORY.arrayNode().add("value"));
            schemasNode.set(aspect + ASPECT_RESPONSE_SUFFIX, responseWrapper);

            ObjectNode requestProperties = NODE_FACTORY.objectNode();
            requestProperties.set("value", aspectRef);

            ObjectNode requestWrapper = NODE_FACTORY.objectNode()
                    .put("type", "object")
                    .put("description", "Aspect wrapper object.")
                    .set("properties", requestProperties);
            requestWrapper.set("required", NODE_FACTORY.arrayNode().add("value"));
            schemasNode.set(aspect + ASPECT_REQUEST_SUFFIX, requestWrapper);
        }).collect(Collectors.toSet());
    }

    private ObjectNode buildEntitySchema(Entity entity, Set<String> aspectDefinitions, boolean isResponse) {
        ObjectNode propertiesNode = NODE_FACTORY.objectNode();

        propertiesNode.set("urn", NODE_FACTORY.objectNode()
                .put("description", "Unique id for " + entity.getName())
                .put("type", "string"));

        propertiesNode.set(entity.getKeyAspect(), buildAspectRef(entity.getKeyAspect(), isResponse));

        entity.getAspects().stream()
                .filter(aspect -> aspectDefinitions.contains(toUpperFirst(aspect))) // Only if aspect is defined
                .forEach(aspect -> propertiesNode.set(aspect, buildAspectRef(aspect, isResponse)));

        ObjectNode entityNode = NODE_FACTORY.objectNode()
                .put("type", "object")
                .put("description", Optional.ofNullable(entity.getDoc())
                        .orElse(toUpperFirst(entity.getName()) + " object."))
                .set("properties", propertiesNode);
        entityNode.set("required", NODE_FACTORY.arrayNode().add("urn"));

        return entityNode;
    }

    private ObjectNode buildEntityScrollSchema(Entity entity) {
        ObjectNode scrollResponsePropertiesNode = NODE_FACTORY.objectNode();

        scrollResponsePropertiesNode.set("scrollId", NODE_FACTORY.objectNode()
                .put("description", "Scroll id for pagination.")
                .put("type", "string"));

        scrollResponsePropertiesNode.set("entities", NODE_FACTORY.objectNode()
                .put("description", Optional.ofNullable(entity.getDoc())
                        .orElse(toUpperFirst(entity.getName()) + " object."))
                .put("type", "array")
                .set("items", NODE_FACTORY.objectNode().put("$ref",
                        String.format("#/components/schemas/%s%s", toUpperFirst(entity.getName()), ENTITY_RESPONSE_SUFFIX))));

        ObjectNode scrollResponseNode = NODE_FACTORY.objectNode()
                .put("type", "object")
                .put("description", "Scroll across " + toUpperFirst(entity.getName()) + " objects.")
                .set("properties", scrollResponsePropertiesNode);
        scrollResponseNode.set("required", NODE_FACTORY.arrayNode().add("entities"));

        return scrollResponseNode;
    }


    private ObjectNode buildAspectRef(String aspect, boolean withSystemMetadata) {
        if (withSystemMetadata) {
            return NODE_FACTORY.objectNode()
                    .put("$ref", String.format("#/definitions/%s%s", toUpperFirst(aspect), ASPECT_RESPONSE_SUFFIX));
        } else {
            return NODE_FACTORY.objectNode()
                    .put("$ref", String.format("#/definitions/%s%s", toUpperFirst(aspect), ASPECT_REQUEST_SUFFIX));
        }
    }

    private Optional<Pair<String, ObjectNode>> generateEntityParameters(final Entity entity, Set<String> definitions) {
    /*
      If not missing key
    */
        if (definitions.contains(toUpperFirst(entity.getKeyAspect()))) {
            final String parameterName = toUpperFirst(entity.getName()) + "Aspects";

            ArrayNode aspects = NODE_FACTORY.arrayNode();
            entity.getAspects().stream()
                    .filter(aspect -> definitions.contains(toUpperFirst(aspect))) // Only if aspect is defined
                    .distinct()
                    .forEach(aspects::add);

            if (aspects.isEmpty()) {
                aspects.add(entity.getKeyAspect());
            }

            ObjectNode itemsNode = NODE_FACTORY.objectNode()
                    .put("type", "string");
            itemsNode.set("enum", aspects);
            itemsNode.set("default", aspects);

            ObjectNode schemaNode = NODE_FACTORY.objectNode()
                    .put("type", "array")
                    .set("items", itemsNode);
            ObjectNode parameterSchemaNode = NODE_FACTORY.objectNode()
                    .put("in", "query")
                    .put("name", "aspects")
                    .put("explode", true)
                    .put("description", "Aspects to include in response.")
                    .set("schema", schemaNode);

            parameterSchemaNode.set("example", aspects);

            ObjectNode parameterNode = NODE_FACTORY.objectNode()
                    .set(parameterName + MODEL_VERSION, parameterSchemaNode);

            return Optional.of(Pair.of(parameterName, parameterNode));
        }

        return Optional.empty();
    }

    private Pair<ObjectNode, Set<String>> buildParameters(ObjectNode schemasNode, Set<String> definitions) {
        ObjectNode parametersNode = NODE_FACTORY.objectNode();
        Set<String> parameterDefinitions =  entityMap.values().stream()
                .flatMap(entity -> generateEntityParameters(entity, definitions).stream())
                .map(entityNode -> {
                    parametersNode.setAll(entityNode.right());
                    return entityNode.left();
                })
                .collect(Collectors.toSet());

        return Pair.of(extraParameters(parametersNode), parameterDefinitions);
    }

    private ObjectNode writeComponentsYaml(ObjectNode schemasNode, ObjectNode parametersNode) throws IOException {
        ObjectNode componentsNode = NODE_FACTORY.objectNode();
        componentsNode.set("schemas", schemasNode);
        componentsNode.set("parameters", extraParameters(parametersNode));
        ObjectNode componentsDocNode = NODE_FACTORY.objectNode().set("components", componentsNode);

        final String componentsYaml = new YAMLMapper().writeValueAsString(componentsDocNode)
                .replaceAll("definitions", "components/schemas")
                .replaceAll("\n\\s+description: null", "")
                .replaceAll("\n\\s+- type: \"null\"", "");
        Files.write(Paths.get(combinedDirectory + GenerateJsonSchemaTask.sep + "open-api-components.yaml"),
                componentsYaml.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        return componentsDocNode;
    }

    private ObjectNode extraParameters(ObjectNode parametersNode) {
        parametersNode.set("ScrollId" + MODEL_VERSION, NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "scrollId")
                .put("description", "Scroll pagination token.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "string")));

        ArrayNode sortFields = NODE_FACTORY.arrayNode();
        sortFields.add("urn");
        ObjectNode sortFieldsNode = NODE_FACTORY.objectNode()
                .put("type", "string");
        sortFieldsNode.set("enum", sortFields);
        sortFieldsNode.set("default", sortFields.get(0));

        ObjectNode sortFieldsSchemaNode = NODE_FACTORY.objectNode()
                .put("type", "array")
                .put("default", "urn")
                .set("items", sortFieldsNode);
        parametersNode.set("SortBy" + MODEL_VERSION, NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "sort")
                .put("explode", true)
                .put("description", "Sort fields for pagination.")
                .put("example", "urn")
                .set("schema", sortFieldsSchemaNode));

        parametersNode.set("SortOrder" + MODEL_VERSION, NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "sortOrder")
                .put("explode", true)
                .put("description", "Sort direction field for pagination.")
                .put("example", "ASCENDING")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("default", "ASCENDING")
                        .put("$ref", "#/components/schemas/SortOrder")));

        parametersNode.set("PaginationCount" + MODEL_VERSION, NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "count")
                .put("description", "Number of items per page.")
                .put("example", "10")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "integer")
                        .put("default", 10)
                        .put("minimum", 1)));
        parametersNode.set("ScrollQuery" + MODEL_VERSION, NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "query")
                .put("description", "Structured search query.")
                .put("example", "*")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "string")
                        .put("default", "*")));

        return parametersNode;
    }

    private void writePathsYaml(Set<String> modelDefinitions, Set<String> parameterDefinitions) throws IOException {
        ObjectNode pathsNode = NODE_FACTORY.objectNode();

        entityMap.values().stream()
                .filter(e -> modelDefinitions.contains(toUpperFirst(e.getName())))
                .forEach(entity -> {

                    pathsNode.set(String.format("/%s", entity.getName().toLowerCase()),
                            buildListEntityPath(entity, parameterDefinitions));

                    pathsNode.set(String.format("/%s/{urn}", entity.getName().toLowerCase()),
                            buildSingleEntityPath(entity, parameterDefinitions));

                });

        buildEntityAspectPaths(pathsNode, modelDefinitions);

        ObjectNode pathsDocNode = NODE_FACTORY.objectNode().set("paths", pathsNode);

        final String componentsYaml = new YAMLMapper().writeValueAsString(pathsDocNode)
                .replaceAll("\n\\s+- type: \"null\"", "")
                .replaceAll("\n\\s+description: null", "");
        Files.write(Paths.get(combinedDirectory + GenerateJsonSchemaTask.sep + "open-api-paths.yaml"),
                componentsYaml.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void buildEntityAspectPaths(ObjectNode pathsNode, Set<String> modelDefinitions) {
        entityMap.values().stream()
                .filter(e -> modelDefinitions.contains(toUpperFirst(e.getName())))
                .forEach(entity -> {
                    entity.getAspects().stream()
                            .filter(aspect -> SUPPORTED_ASPECT_PATHS.contains(aspect))
                            .filter(aspect -> modelDefinitions.contains(toUpperFirst(aspect)))
                            .forEach(aspect -> pathsNode.set(String.format("/%s/{urn}/%s",
                                            entity.getName().toLowerCase(), aspect.toLowerCase()),
                                    buildSingleEntityAspectPath(entity, aspect)));
                });
    }

    private ObjectNode buildListEntityPath(Entity entity, Set<String> parameterDefinitions) {
        final String upperFirst = toUpperFirst(entity.getName());
        final String aspectParameterName = upperFirst + "Aspects";
        ArrayNode tagsNode = NODE_FACTORY.arrayNode()
                .add(entity.getName() + " Entity");

        ObjectNode scrollMethod = NODE_FACTORY.objectNode()
                .put("summary", String.format("Scroll %s.", upperFirst))
                .put("operationId", String.format("scroll", upperFirst));

        ArrayNode scrollPathParametersNode = NODE_FACTORY.arrayNode();
        scrollMethod.set("parameters", scrollPathParametersNode);
        scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "systemMetadata")
                .put("description", "Include systemMetadata with response.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        if (parameterDefinitions.contains(aspectParameterName)) {
            scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                    .put("$ref", String.format("#/components/parameters/%s", aspectParameterName + MODEL_VERSION)));
        }
        scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                .put("$ref", "#/components/parameters/PaginationCount" + MODEL_VERSION));
        scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                .put("$ref", "#/components/parameters/ScrollId" + MODEL_VERSION));
        scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                .put("$ref", "#/components/parameters/SortBy" + MODEL_VERSION));
        scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                .put("$ref", "#/components/parameters/SortOrder" + MODEL_VERSION));
        scrollPathParametersNode.add(NODE_FACTORY.objectNode()
                .put("$ref", "#/components/parameters/ScrollQuery" + MODEL_VERSION));
        scrollMethod.set("parameters", scrollPathParametersNode);
        scrollMethod.set("responses", NODE_FACTORY.objectNode()
                .set("200", NODE_FACTORY.objectNode().put("description", "Success")
                        .set("content", NODE_FACTORY.objectNode().set("application/json", NODE_FACTORY.objectNode()
                                .set("schema", NODE_FACTORY.objectNode()
                                        .put("$ref", String.format("#/components/schemas/Scroll%s%s", upperFirst, ENTITY_RESPONSE_SUFFIX)))))));
        scrollMethod.set("tags", tagsNode);

        ObjectNode postMethod = NODE_FACTORY.objectNode()
                .put("summary", "Create " + upperFirst)
                .put("operationId", String.format("create", upperFirst));
        ArrayNode postParameters = NODE_FACTORY.arrayNode();
        postMethod.set("parameters", postParameters);
        postParameters.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "createIfNotExists")
                .put("description", "Create the aspect if it does not already exist.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        postParameters.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "createEntityIfNotExists")
                .put("description", "Create the entity ONLY if it does not already exist. Fails in case when the entity exists.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        postMethod.set("requestBody", NODE_FACTORY.objectNode()
                .put("description", "Create " + entity.getName() + " entities.")
                .put("required", true)
                .set("content", NODE_FACTORY.objectNode().set("application/json", NODE_FACTORY.objectNode()
                        .set("schema", NODE_FACTORY.objectNode().put("type", "array")
                                .set("items", NODE_FACTORY.objectNode().put("$ref",
                                        String.format("#/components/schemas/%s%s", upperFirst, ENTITY_REQUEST_SUFFIX)))))));
        postMethod.set("responses", NODE_FACTORY.objectNode()
                .set("201", NODE_FACTORY.objectNode().put("description", "Create " + entity.getName() + " entities.")
                        .set("content", NODE_FACTORY.objectNode().set("application/json", NODE_FACTORY.objectNode()
                                .set("schema", NODE_FACTORY.objectNode().put("type", "array")
                                        .set("items", NODE_FACTORY.objectNode().put("$ref",
                                                String.format("#/components/schemas/%s%s", upperFirst, ENTITY_RESPONSE_SUFFIX))))))));
        postMethod.set("tags", tagsNode);

        ObjectNode listMethods = NODE_FACTORY.objectNode();
        listMethods.set("get", scrollMethod);
        listMethods.set("post", postMethod);

        return listMethods;
    }

    private ObjectNode buildSingleEntityPath(Entity entity, Set<String> parameterDefinitions) {
        final String upperFirst = toUpperFirst(entity.getName());
        final String aspectParameterName = upperFirst + "Aspects";
        ArrayNode tagsNode = NODE_FACTORY.arrayNode().add(entity.getName() + " Entity");

        ObjectNode getMethod = NODE_FACTORY.objectNode()
                .put("summary", String.format("Get %s by key.", entity.getName()))
                .put("operationId", String.format("get", upperFirst));
        getMethod.set("tags", tagsNode);
        ArrayNode singlePathParametersNode = NODE_FACTORY.arrayNode();
        getMethod.set("parameters", singlePathParametersNode);
        singlePathParametersNode.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "systemMetadata")
                .put("description", "Include systemMetadata with response.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        if(parameterDefinitions.contains(aspectParameterName)) {
            singlePathParametersNode.add(NODE_FACTORY.objectNode()
                    .put("$ref", String.format("#/components/parameters/%s", aspectParameterName + MODEL_VERSION)));
        }

        ObjectNode responses = NODE_FACTORY.objectNode();
        getMethod.set("responses", responses);
        responses.set("200", NODE_FACTORY.objectNode().put("description", "Success")
                .set("content", NODE_FACTORY.objectNode().set("application/json", NODE_FACTORY.objectNode()
                        .set("schema", NODE_FACTORY.objectNode().put("$ref",
                                String.format("#/components/schemas/%s%s", upperFirst, ENTITY_RESPONSE_SUFFIX))))));
        responses.set("404", NODE_FACTORY.objectNode()
                .put("description", "Not Found")
                .set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode()
                                .set("schema", NODE_FACTORY.objectNode()))));

        ObjectNode headResponses = NODE_FACTORY.objectNode();
        headResponses.set("204", NODE_FACTORY.objectNode()
                .put("description", entity.getName() + " exists.")
                .set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode())));
        headResponses.set("404", NODE_FACTORY.objectNode()
                .put("description", entity.getName() + " does not exist.")
                .set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode())));
        ObjectNode headMethod = NODE_FACTORY.objectNode()
                .put("summary", upperFirst  + " existence.")
                .put("operationId", String.format("head", upperFirst))
                .set("responses", headResponses);
        headMethod.set("tags", tagsNode);

        ObjectNode deleteMethod = NODE_FACTORY.objectNode()
                .put("summary", "Delete entity " + upperFirst)
                .put("operationId", String.format("delete", upperFirst))
                .set("responses", NODE_FACTORY.objectNode()
                        .set("200", NODE_FACTORY.objectNode()
                                .put("description", "Delete " + entity.getName() + " entity.")
                                .set("content", NODE_FACTORY.objectNode()
                                        .set("application/json", NODE_FACTORY.objectNode()))));
        deleteMethod.set("tags", tagsNode);

        ObjectNode singlePathMethods = NODE_FACTORY.objectNode()
                .set("parameters", NODE_FACTORY.arrayNode()
                        .add(NODE_FACTORY.objectNode()
                                .put("in", "path")
                                .put("name", "urn")
                                .put("required", true)
                                .set("schema", NODE_FACTORY.objectNode().put("type", "string"))));
        singlePathMethods.set("get", getMethod);
        singlePathMethods.set("head", headMethod);
        singlePathMethods.set("delete", deleteMethod);

        return singlePathMethods;
    }

    private ObjectNode buildSingleEntityAspectPath(Entity entity, String aspect) {
        final String upperFirstEntity = toUpperFirst(entity.getName());
        final String upperFirstAspect = toUpperFirst(aspect);

        ArrayNode tagsNode = NODE_FACTORY.arrayNode()
                .add(aspect + " Aspect");

        ObjectNode getMethod = NODE_FACTORY.objectNode()
                .put("summary", String.format("Get %s for %s.", aspect, entity.getName()))
                .put("operationId", String.format("get%s", upperFirstAspect));
        getMethod.set("tags", tagsNode);
        ArrayNode singlePathParametersNode = NODE_FACTORY.arrayNode();
        getMethod.set("parameters", singlePathParametersNode);
        singlePathParametersNode.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "systemMetadata")
                .put("description", "Include systemMetadata with response.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        getMethod.set("responses", NODE_FACTORY.objectNode().set("200", NODE_FACTORY.objectNode()
                .put("description", "Success").set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode().set("schema", NODE_FACTORY.objectNode()
                                .put("$ref",
                                        String.format("#/components/schemas/%s%s", upperFirstAspect, ASPECT_RESPONSE_SUFFIX)))))));

        ObjectNode headResponses = NODE_FACTORY.objectNode();
        headResponses.set("200", NODE_FACTORY.objectNode()
                .put("description", String.format("%s on %s exists.", aspect, entity.getName()))
                .set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode())));
        headResponses.set("404", NODE_FACTORY.objectNode()
                .put("description", String.format("%s on %s does not exist.", aspect, entity.getName()))
                .set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode())));
        ObjectNode headMethod = NODE_FACTORY.objectNode()
                .put("summary", String.format("%s on %s existence.", aspect, upperFirstEntity))
                .put("operationId", String.format("head%s", upperFirstAspect))
                .set("responses", headResponses);
        headMethod.set("tags", tagsNode);

        ObjectNode deleteMethod = NODE_FACTORY.objectNode()
                .put("summary", String.format("Delete %s on entity %s", aspect, upperFirstEntity))
                .put("operationId", String.format("delete%s", upperFirstAspect))
                .set("responses", NODE_FACTORY.objectNode()
                        .set("200", NODE_FACTORY.objectNode()
                                .put("description", String.format("Delete %s on %s entity.", aspect, upperFirstEntity))
                                .set("content", NODE_FACTORY.objectNode()
                                        .set("application/json", NODE_FACTORY.objectNode()))));
        deleteMethod.set("tags", tagsNode);

        ObjectNode postMethod = NODE_FACTORY.objectNode()
                .put("summary", String.format("Create aspect %s on %s ", aspect, upperFirstEntity))
                .put("operationId", String.format("create%s", upperFirstAspect));
        ArrayNode postParameters = NODE_FACTORY.arrayNode();
        postMethod.set("parameters", postParameters);
        postParameters.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "createIfNotExists")
                .put("description", "Create the aspect if it does not already exist.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        postParameters.add(NODE_FACTORY.objectNode()
                .put("in", "query")
                .put("name", "createEntityIfNotExists")
                .put("description", "Create the entity if it does not already exist. Fails in case when the entity exists.")
                .set("schema", NODE_FACTORY.objectNode()
                        .put("type", "boolean")
                        .put("default", false)));
        postMethod.set("requestBody", NODE_FACTORY.objectNode()
                .put("description", String.format("Create aspect %s on %s entity.", aspect, upperFirstEntity))
                .put("required", true).set("content", NODE_FACTORY.objectNode()
                        .set("application/json", NODE_FACTORY.objectNode().set("schema", NODE_FACTORY.objectNode()
                                .put("$ref",
                                        String.format("#/components/schemas/%s%s", upperFirstAspect, ASPECT_REQUEST_SUFFIX))))));
        postMethod.set("responses", NODE_FACTORY.objectNode().set("201", NODE_FACTORY.objectNode()
                .put("description", String.format("Create aspect %s on %s entity.", aspect, upperFirstEntity))
                .set("content", NODE_FACTORY.objectNode().set("application/json", NODE_FACTORY.objectNode()
                        .set("schema", NODE_FACTORY.objectNode().put("$ref",
                                String.format("#/components/schemas/%s%s", upperFirstAspect, ASPECT_RESPONSE_SUFFIX)))))));
        postMethod.set("tags", tagsNode);

        ObjectNode singlePathMethods = NODE_FACTORY.objectNode()
                .set("parameters", NODE_FACTORY.arrayNode()
                        .add(NODE_FACTORY.objectNode()
                                .put("in", "path")
                                .put("name", "urn")
                                .put("required", true)
                                .set("schema", NODE_FACTORY.objectNode().put("type", "string"))));
        singlePathMethods.set("get", getMethod);
        singlePathMethods.set("head", headMethod);
        singlePathMethods.set("delete", deleteMethod);
        singlePathMethods.set("post", postMethod);

        return singlePathMethods;
    }
}
