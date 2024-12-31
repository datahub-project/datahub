package com.linkedin.metadata.models;

import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityProfile;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.processing.ProcessingUtil;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.avro.SchemaTranslator;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.TestConstants;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SuppressWarnings({"rawtypes", "unchecked"})
public class OpenApiSpecBuilderTest {
  private static final String MODEL_VERSION = "_v3";
  private static final String TYPE_OBJECT = "object";
  private static final String TYPE_BOOLEAN = "boolean";
  private static final String TYPE_STRING = "string";
  private static final String TYPE_ARRAY = "array";
  private static final String TYPE_INTEGER = "integer";
  private static final String NAME_QUERY = "query";
  private static final String NAME_SYSTEM_METADATA = "systemMetadata";
  private static final String NAME_SCROLL_ID = "scrollId";
  private static final String PROPERTY_VALUE = "value";
  private static final String PROPERTY_URN = "urn";
  private static final String PATH_DEFINITIONS = "#/components/schemas/";
  private static final String FORMAT_PATH_DEFINITIONS = "#/components/schemas/%s%s";
  private static final String ASPECT_DESCRIPTION = "Aspect wrapper object.";
  private static final String REQUEST_SUFFIX = "Request" + MODEL_VERSION;
  private static final String RESPONSE_SUFFIX = "Response" + MODEL_VERSION;

  private static final String ASPECT_REQUEST_SUFFIX = "Aspect" + REQUEST_SUFFIX;
  private static final String ASPECT_RESPONSE_SUFFIX = "Aspect" + RESPONSE_SUFFIX;
  private static final String ENTITY_REQUEST_SUFFIX = "Entity" + REQUEST_SUFFIX;
  private static final String ENTITY_RESPONSE_SUFFIX = "Entity" + RESPONSE_SUFFIX;
  private static final ImmutableSet<Object> SUPPORTED_ASPECT_PATHS =
      ImmutableSet.builder()
          .add("domains")
          .add("ownership")
          .add("deprecation")
          .add("status")
          .add("globalTags")
          .add("glossaryTerms")
          .add("dataContractInfo")
          .add("browsePathsV2")
          .add("datasetProperties")
          .add("editableDatasetProperties")
          .add("chartInfo")
          .add("editableChartProperties")
          .add("dashboardInfo")
          .add("editableDashboardProperties")
          .add("notebookInfo")
          .add("editableNotebookProperties")
          .add("dataProductProperties")
          .add("institutionalMemory")
          .build();

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testOpenApiSpecBuilder() throws Exception {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    MergedEntityRegistry er = new MergedEntityRegistry(configEntityRegistry);
    new PluginEntityRegistryLoader(TestConstants.BASE_DIRECTORY, 1, null)
        .withBaseRegistry(er)
        .start(true);

    OpenAPI openAPI = generateOpenApiSpec(er);
    String openapiYaml = Yaml.pretty(openAPI);
    Files.write(
        Path.of(getClass().getResource("/").getPath(), "open-api.yaml"),
        openapiYaml.getBytes(StandardCharsets.UTF_8));

    assertTrue(openAPI.getComponents().getSchemas().size() >= 882);
    assertTrue(openAPI.getComponents().getParameters().size() >= 54);
    assertTrue(openAPI.getPaths().size() >= 98);
  }

  private OpenAPI generateOpenApiSpec(EntityRegistry entityRegistry) {
    final Set<String> aspectNames = entityRegistry.getAspectSpecs().keySet();
    final Set<String> entityNames =
        entityRegistry.getEntitySpecs().values().stream()
            .filter(e -> aspectNames.contains(e.getKeyAspectName()))
            .map(EntitySpec::getName)
            .collect(Collectors.toSet());
    final Set<String> definitionNames =
        Stream.concat(aspectNames.stream(), entityNames.stream()).collect(Collectors.toSet());
    // Info
    final Info info = new Info();
    info.setTitle("Entity API");
    info.setDescription("This is a service for DataHub Entities.");
    info.setVersion("v3");
    // Components
    final Components components = new Components();
    // --> Aspect components
    // TODO: Correct handling of SystemMetadata and SortOrder
    components.addSchemas("SystemMetadata", new Schema().type(TYPE_STRING));
    components.addSchemas("SortOrder", new Schema()._enum(List.of("ASCENDING", "DESCENDING")));
    entityRegistry
        .getAspectSpecs()
        .values()
        .forEach(
            a -> {
              final String upperAspectName = a.getPegasusSchema().getName();
              addAspectSchemas(components, a);
              components.addSchemas(
                  upperAspectName + ASPECT_REQUEST_SUFFIX,
                  buildAspectRefSchema(upperAspectName, false));
              components.addSchemas(
                  upperAspectName + ASPECT_RESPONSE_SUFFIX,
                  buildAspectRefSchema(upperAspectName, true));
            });
    // --> Entity components
    entityRegistry.getEntitySpecs().values().stream()
        .filter(e -> aspectNames.contains(e.getKeyAspectName()))
        .forEach(
            e -> {
              final String entityName = toUpperFirst(e.getName());
              components.addSchemas(
                  entityName + ENTITY_REQUEST_SUFFIX, buildEntitySchema(e, aspectNames, false));
              components.addSchemas(
                  entityName + ENTITY_RESPONSE_SUFFIX, buildEntitySchema(e, aspectNames, true));
              components.addSchemas(
                  "Scroll" + entityName + ENTITY_RESPONSE_SUFFIX, buildEntityScrollSchema(e));
            });
    // Parameters
    entityRegistry.getEntitySpecs().values().stream()
        .filter(e -> definitionNames.contains(e.getKeyAspectName()))
        .forEach(
            e -> {
              final String parameterName = toUpperFirst(e.getName()) + "Aspects";
              components.addParameters(
                  parameterName + MODEL_VERSION, buildParameterSchema(e, definitionNames));
            });
    addExtraParameters(components);
    // Path
    final Paths paths = new Paths();
    entityRegistry.getEntitySpecs().values().stream()
        .filter(e -> definitionNames.contains(e.getName()))
        .forEach(
            e -> {
              paths.addPathItem(
                  String.format("/%s", e.getName().toLowerCase()), buildListEntityPath(e));
              paths.addPathItem(
                  String.format("/%s/{urn}", e.getName().toLowerCase()), buildListEntityPath(e));
            });
    entityRegistry.getEntitySpecs().values().stream()
        .filter(e -> definitionNames.contains(e.getName()))
        .forEach(
            e -> {
              e.getAspectSpecs().stream()
                  .filter(a -> SUPPORTED_ASPECT_PATHS.contains(a.getName()))
                  .filter(a -> definitionNames.contains(toUpperFirst(a.getName())))
                  .forEach(
                      a ->
                          paths.addPathItem(
                              String.format(
                                  "/%s/{urn}/%s",
                                  e.getName().toLowerCase(), a.getName().toLowerCase()),
                              buildSingleEntityAspectPath(
                                  e, a.getName(), a.getPegasusSchema().getName())));
            });
    return new OpenAPI().openapi("3.0.1").info(info).paths(paths).components(components);
  }

  private PathItem buildListEntityPath(final EntitySpec entity) {
    final String upperFirst = toUpperFirst(entity.getName());
    final String aspectParameterName = upperFirst + "Aspects";
    final PathItem result = new PathItem();
    final List<Parameter> parameters =
        List.of(
            new Parameter()
                .in(NAME_QUERY)
                .name("systemMetadata")
                .description("Include systemMetadata with response.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .$ref(
                    String.format(
                        "#/components/parameters/%s", aspectParameterName + MODEL_VERSION)),
            new Parameter().$ref("#/components/parameters/PaginationCount" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/ScrollId" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/SortBy" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/SortOrder" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/ScrollQuery" + MODEL_VERSION));
    final ApiResponse successApiResponse =
        new ApiResponse()
            .description("Success")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/Scroll%s%s",
                                            upperFirst, ENTITY_RESPONSE_SUFFIX)))));
    result.setGet(
        new Operation()
            .summary(String.format("Scroll %s.", upperFirst))
            .operationId("scroll")
            .parameters(parameters)
            .tags(List.of(entity.getName() + " Entity"))
            .responses(new ApiResponses().addApiResponse("200", successApiResponse)));
    final Content requestContent =
        new Content()
            .addMediaType(
                "application/json",
                new MediaType()
                    .schema(
                        new Schema()
                            .type(TYPE_ARRAY)
                            .items(
                                new Schema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            upperFirst, ENTITY_REQUEST_SUFFIX)))));
    final ApiResponse apiResponse =
        new ApiResponse()
            .description("Create " + entity.getName() + " entities.")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema()
                                    .type(TYPE_ARRAY)
                                    .items(
                                        new Schema<>()
                                            .$ref(
                                                String.format(
                                                    "#/components/schemas/%s%s",
                                                    upperFirst, ENTITY_RESPONSE_SUFFIX))))));
    result.setPost(
        new Operation()
            .summary("Create " + upperFirst)
            .operationId("create")
            .tags(List.of(entity.getName() + " Entity"))
            .requestBody(
                new RequestBody()
                    .description("Create " + entity.getName() + " entities.")
                    .required(true)
                    .content(requestContent))
            .responses(new ApiResponses().addApiResponse("201", apiResponse)));
    return result;
  }

  private void addExtraParameters(final Components components) {
    components.addParameters(
        "ScrollId" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_SCROLL_ID)
            .description("Scroll pagination token.")
            .schema(new Schema().type(TYPE_STRING)));
    components.addParameters(
        "SortBy" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name("sort")
            .explode(true)
            .description("Sort fields for pagination.")
            .example(PROPERTY_URN)
            .schema(
                new Schema()
                    .type(TYPE_ARRAY)
                    ._default(PROPERTY_URN)
                    .items(
                        new Schema<>()
                            .type(TYPE_STRING)
                            ._enum(List.of(PROPERTY_URN))
                            ._default(PROPERTY_URN))));
    components.addParameters(
        "SortOrder" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name("sortOrder")
            .explode(true)
            .description("Sort direction field for pagination.")
            .example("ASCENDING")
            .schema(new Schema()._default("ASCENDING").$ref("#/components/schemas/SortOrder")));
    components.addParameters(
        "PaginationCount" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name("count")
            .description("Number of items per page.")
            .example("10")
            .schema(new Schema().type(TYPE_INTEGER)._default(10).minimum(new BigDecimal(1))));
    components.addParameters(
        "ScrollQuery" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_QUERY)
            .description("Structured search query.")
            .example("*")
            .schema(new Schema().type(TYPE_STRING)._default("*")));
  }

  private Parameter buildParameterSchema(
      final EntitySpec entity, final Set<String> definitionNames) {
    final List<String> aspectNames =
        entity.getAspectSpecs().stream()
            .map(AspectSpec::getName)
            .filter(definitionNames::contains) // Only if aspect is defined
            .distinct()
            .collect(Collectors.toList());
    if (aspectNames.isEmpty()) {
      aspectNames.add(entity.getKeyAspectName());
    }
    final Schema schema =
        new Schema()
            .type(TYPE_ARRAY)
            .items(new Schema().type(TYPE_STRING)._enum(aspectNames)._default(aspectNames));
    return new Parameter()
        .in(NAME_QUERY)
        .name("aspects")
        .explode(true)
        .description("Aspects to include in response.")
        .example(aspectNames)
        .schema(schema);
  }

  private void addAspectSchemas(final Components components, final AspectSpec aspect) {
    final org.apache.avro.Schema avroSchema =
        SchemaTranslator.dataToAvroSchema(aspect.getPegasusSchema().getDereferencedDataSchema());
    try {
      final JsonNode apiSchema = ProcessingUtil.buildResult(avroSchema.toString());
      final JsonNode definitions = apiSchema.get("definitions");
      definitions
          .fieldNames()
          .forEachRemaining(
              n -> {
                try {
                  final String definition = Json.mapper().writeValueAsString(definitions.get(n));
                  final String newDefinition =
                      definition.replaceAll("definitions", "components/schemas");
                  Schema s = Json.mapper().readValue(newDefinition, Schema.class);
                  components.addSchemas(n, s);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Schema buildAspectRefSchema(final String aspectName, final boolean withSystemMetadata) {
    final Schema result =
        new Schema<>()
            .type(TYPE_OBJECT)
            .description(ASPECT_DESCRIPTION)
            .required(List.of(PROPERTY_VALUE))
            .addProperty(PROPERTY_VALUE, new Schema<>().$ref(PATH_DEFINITIONS + aspectName));
    if (withSystemMetadata) {
      result.addProperty(
          "systemMetadata",
          new Schema<>()
              .$ref(PATH_DEFINITIONS + "SystemMetadata")
              .description("System metadata for the aspect."));
    }
    return result;
  }

  private Schema buildEntitySchema(
      final EntitySpec entity, Set<String> aspectNames, final boolean withSystemMetadata) {
    final Map<String, Schema> properties =
        entity.getAspectSpecMap().entrySet().stream()
            .filter(a -> aspectNames.contains(a.getValue().getName()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    a ->
                        buildAspectRef(
                            a.getValue().getPegasusSchema().getName(), withSystemMetadata)));
    properties.put(
        PROPERTY_URN,
        new Schema<>().type(TYPE_STRING).description("Unique id for " + entity.getName()));
    properties.put(
        entity.getKeyAspectName(),
        buildAspectRef(entity.getKeyAspectSpec().getPegasusSchema().getName(), withSystemMetadata));
    return new Schema<>()
        .type(TYPE_OBJECT)
        .description(toUpperFirst(entity.getName()) + " object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  private Schema buildEntityScrollSchema(final EntitySpec entity) {
    return new Schema<>()
        .type(TYPE_OBJECT)
        .description("Scroll across " + toUpperFirst(entity.getName()) + " objects.")
        .required(List.of("entities"))
        .addProperty(
            NAME_SCROLL_ID,
            new Schema<>().type(TYPE_STRING).description("Scroll id for pagination."))
        .addProperty(
            "entities",
            new Schema<>()
                .type(TYPE_ARRAY)
                .description(toUpperFirst(entity.getName()) + " object.")
                .items(
                    new Schema<>()
                        .$ref(
                            String.format(
                                "#/components/schemas/%s%s",
                                toUpperFirst(entity.getName()), ENTITY_RESPONSE_SUFFIX))));
  }

  private Schema buildAspectRef(final String aspect, final boolean withSystemMetadata) {
    final Schema result = new Schema<>();
    if (withSystemMetadata) {
      result.set$ref(
          String.format(FORMAT_PATH_DEFINITIONS, toUpperFirst(aspect), ASPECT_RESPONSE_SUFFIX));
    } else {
      result.set$ref(
          String.format(FORMAT_PATH_DEFINITIONS, toUpperFirst(aspect), ASPECT_REQUEST_SUFFIX));
    }
    return result;
  }

  private PathItem buildSingleEntityAspectPath(
      final EntitySpec entity, final String aspect, final String upperFirstAspect) {
    final String upperFirstEntity = toUpperFirst(entity.getName());

    List<String> tags = List.of(aspect + " Aspect");
    // Get Operation
    final Parameter getParameter =
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_SYSTEM_METADATA)
            .description("Include systemMetadata with response.")
            .schema(new Schema().type(TYPE_BOOLEAN)._default(false));
    final ApiResponse successApiResponse =
        new ApiResponse()
            .description("Success")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            upperFirstAspect, ASPECT_RESPONSE_SUFFIX)))));
    final Operation getOperation =
        new Operation()
            .summary(String.format("Get %s for %s.", aspect, entity.getName()))
            .operationId(String.format("get%s", upperFirstAspect))
            .tags(tags)
            .parameters(List.of(getParameter))
            .responses(new ApiResponses().addApiResponse("200", successApiResponse));
    // Head Operation
    final ApiResponse successHeadResponse =
        new ApiResponse()
            .description(String.format("%s on %s exists.", aspect, entity.getName()))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final ApiResponse notFoundHeadResponse =
        new ApiResponse()
            .description(String.format("%s on %s does not exist.", aspect, entity.getName()))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final Operation headOperation =
        new Operation()
            .summary(String.format("%s on %s existence.", aspect, upperFirstEntity))
            .operationId(String.format("head%s", upperFirstAspect))
            .tags(tags)
            .responses(
                new ApiResponses()
                    .addApiResponse("200", successHeadResponse)
                    .addApiResponse("404", notFoundHeadResponse));
    // Delete Operation
    final ApiResponse successDeleteResponse =
        new ApiResponse()
            .description(String.format("Delete %s on %s entity.", aspect, upperFirstEntity))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final Operation deleteOperation =
        new Operation()
            .summary(String.format("Delete %s on entity %s", aspect, upperFirstEntity))
            .operationId(String.format("delete%s", upperFirstAspect))
            .tags(tags)
            .responses(new ApiResponses().addApiResponse("200", successDeleteResponse));
    // Post Operation
    final ApiResponse successPostResponse =
        new ApiResponse()
            .description(String.format("Create aspect %s on %s entity.", aspect, upperFirstEntity))
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            upperFirstAspect, ASPECT_RESPONSE_SUFFIX)))));
    final RequestBody requestBody =
        new RequestBody()
            .description(String.format("Create aspect %s on %s entity.", aspect, upperFirstEntity))
            .required(true)
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            upperFirstAspect, ASPECT_REQUEST_SUFFIX)))));
    final Operation postOperation =
        new Operation()
            .summary(String.format("Create aspect %s on %s ", aspect, upperFirstEntity))
            .operationId(String.format("create%s", upperFirstAspect))
            .tags(tags)
            .requestBody(requestBody)
            .responses(new ApiResponses().addApiResponse("201", successPostResponse));
    return new PathItem()
        .parameters(
            List.of(
                new Parameter()
                    .in("path")
                    .name("urn")
                    .required(true)
                    .schema(new Schema().type(TYPE_STRING))))
        .get(getOperation)
        .head(headOperation)
        .delete(deleteOperation)
        .post(postOperation);
  }

  private static String toUpperFirst(final String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1);
  }
}
