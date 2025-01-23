package io.datahubproject.openapi.v3;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static io.datahubproject.openapi.util.ReflectionCache.toUpperFirst;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.processing.ProcessingUtil;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.avro.SchemaTranslator;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.*;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"rawtypes", "unchecked"})
public class OpenAPIV3Generator {
  private static final String MODEL_VERSION = "_v3";
  private static final String TYPE_OBJECT = "object";
  private static final String TYPE_BOOLEAN = "boolean";
  private static final String TYPE_STRING = "string";
  private static final String TYPE_ARRAY = "array";
  private static final String TYPE_INTEGER = "integer";
  private static final String NAME_QUERY = "query";
  private static final String NAME_PATH = "path";
  private static final String NAME_SYSTEM_METADATA = "systemMetadata";
  private static final String NAME_AUDIT_STAMP = "auditStamp";
  private static final String NAME_VERSION = "version";
  private static final String NAME_SCROLL_ID = "scrollId";
  private static final String NAME_INCLUDE_SOFT_DELETE = "includeSoftDelete";
  private static final String NAME_PIT_KEEP_ALIVE = "pitKeepAlive";
  private static final String PROPERTY_VALUE = "value";
  private static final String PROPERTY_URN = "urn";
  private static final String PROPERTY_PATCH = "patch";
  private static final String PROPERTY_PATCH_PKEY = "arrayPrimaryKeys";
  private static final String PATH_DEFINITIONS = "#/components/schemas/";
  private static final String FORMAT_PATH_DEFINITIONS = "#/components/schemas/%s%s";
  private static final String ASPECT_DESCRIPTION = "Aspect wrapper object.";
  private static final String REQUEST_SUFFIX = "Request" + MODEL_VERSION;
  private static final String RESPONSE_SUFFIX = "Response" + MODEL_VERSION;

  private static final String ASPECT_REQUEST_SUFFIX = "Aspect" + REQUEST_SUFFIX;
  private static final String ASPECT_RESPONSE_SUFFIX = "Aspect" + RESPONSE_SUFFIX;
  private static final String ENTITY_REQUEST_SUFFIX = "Entity" + REQUEST_SUFFIX;
  private static final String ENTITY_RESPONSE_SUFFIX = "Entity" + RESPONSE_SUFFIX;
  private static final String NAME_SKIP_CACHE = "skipCache";
  private static final String ASPECTS = "Aspects";
  private static final String ENTITIES = "Entities";

  public static OpenAPI generateOpenApiSpec(
      EntityRegistry entityRegistry, ConfigurationProvider configurationProvider) {
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

    // Cross-entity components
    components.addSchemas(
        ENTITIES + ENTITY_REQUEST_SUFFIX, buildEntitiesRequestSchema(entityRegistry, aspectNames));
    components.addSchemas(
        ENTITIES + ENTITY_RESPONSE_SUFFIX, buildEntitySchema(entityRegistry, aspectNames, true));
    components.addSchemas(
        "Scroll" + ENTITIES + ENTITY_RESPONSE_SUFFIX, buildEntitiesScrollSchema());

    // --> Aspect components
    components.addSchemas("AspectPatch", buildAspectPatchSchema());
    components.addSchemas(
        "BatchGetRequestBody",
        new Schema<>()
            .type(TYPE_OBJECT)
            .description("Request body for batch get aspects.")
            .properties(
                Map.of(
                    "headers",
                    new Schema<>()
                        .type(TYPE_OBJECT)
                        .additionalProperties(new Schema<>().type(TYPE_STRING))
                        .description("System headers for the operation.")
                        .nullable(true)))
            .nullable(true));

    // --> Aspect components
    components.addSchemas(
        ASPECTS + ASPECT_RESPONSE_SUFFIX, buildAspectsRefResponseSchema(entityRegistry));
    entityRegistry
        .getAspectSpecs()
        .values()
        .forEach(
            a -> {
              final String upperAspectName = a.getPegasusSchema().getName();
              addAspectSchemas(components, a);
              components.addSchemas(
                  upperAspectName + ASPECT_REQUEST_SUFFIX,
                  buildAspectRefRequestSchema(upperAspectName));
              components.addSchemas(
                  upperAspectName + ASPECT_RESPONSE_SUFFIX,
                  buildAspectRefResponseSchema(upperAspectName));
            });

    List<EntitySpec> definedEntitySpecs =
        entityRegistry.getEntitySpecs().values().stream()
            .filter(entitySpec -> definitionNames.contains(entitySpec.getName()))
            .sorted(Comparator.comparing(EntitySpec::getName))
            .collect(Collectors.toList());
    // --> Entity components
    definedEntitySpecs.forEach(
        e -> {
          final String entityName = toUpperFirst(e.getName());
          components.addSchemas(
              entityName + ENTITY_REQUEST_SUFFIX, buildEntitySchema(e, aspectNames, false));
          components.addSchemas(
              entityName + ENTITY_RESPONSE_SUFFIX, buildEntitySchema(e, aspectNames, true));
          components.addSchemas(
              "Scroll" + entityName + ENTITY_RESPONSE_SUFFIX, buildEntityScrollSchema(e));
          components.addSchemas(
              "BatchGet" + entityName + ENTITY_REQUEST_SUFFIX,
              buildEntityBatchGetRequestSchema(e, aspectNames));
        });

    components.addSchemas("SortOrder", new Schema()._enum(List.of("ASCENDING", "DESCENDING")));
    // TODO: Correct handling of SystemMetadata and AuditStamp
    components.addSchemas(
        "SystemMetadata", new Schema().type(TYPE_OBJECT).additionalProperties(true));
    components.addSchemas("AuditStamp", new Schema().type(TYPE_OBJECT).additionalProperties(true));

    // Parameters

    // --> Entity Parameters
    definedEntitySpecs.forEach(
        e -> {
          final String parameterName = toUpperFirst(e.getName()) + ASPECTS;
          components.addParameters(
              parameterName + MODEL_VERSION, buildParameterSchema(e, definitionNames));
        });

    addExtraParameters(components);

    // Path
    final Paths paths = new Paths();

    // --> Cross-entity Paths
    paths.addPathItem("/v3/entity/scroll", buildGenericListEntitiesPath());

    // --> Entity Paths
    definedEntitySpecs.forEach(
        e -> {
          paths.addPathItem(
              String.format("/v3/entity/%s", e.getName().toLowerCase()), buildListEntityPath(e));
          paths.addPathItem(
              String.format("/v3/entity/%s/batchGet", e.getName().toLowerCase()),
              buildBatchGetEntityPath(e));
          paths.addPathItem(
              String.format("/v3/entity/%s/{urn}", e.getName().toLowerCase()),
              buildSingleEntityPath(e));
        });

    // --> Aspect Paths
    definedEntitySpecs.forEach(
        e ->
            e.getAspectSpecs().stream()
                .filter(a -> definitionNames.contains(a.getName()))
                .sorted(Comparator.comparing(AspectSpec::getName))
                .forEach(
                    a ->
                        paths.addPathItem(
                            String.format(
                                "/v3/entity/%s/{urn}/%s",
                                e.getName().toLowerCase(), a.getName().toLowerCase()),
                            buildSingleEntityAspectPath(e, a))));
    definedEntitySpecs.forEach(
        e ->
            e.getAspectSpecs().stream()
                .filter(a -> definitionNames.contains(a.getName()))
                .sorted(Comparator.comparing(AspectSpec::getName))
                .forEach(
                    a ->
                        paths.addPathItem(
                            String.format(
                                "/v3/entity/%s/{urn}/%s",
                                e.getName().toLowerCase(), a.getName().toLowerCase()),
                            buildSingleEntityAspectPath(e, a))));

    // --> Link & Unlink APIs
    if (configurationProvider.getFeatureFlags().isEntityVersioning()) {
      definedEntitySpecs.stream()
          .filter(entitySpec -> VERSION_SET_ENTITY_NAME.equals(entitySpec.getName()))
          .forEach(
              entitySpec -> {
                paths.addPathItem(
                    "/v3/entity/versioning/{versionSetUrn}/relationship/versionOf/{entityUrn}",
                    buildVersioningRelationshipPath());
              });
    }

    return new OpenAPI().openapi("3.0.1").info(info).paths(paths).components(components);
  }

  private static PathItem buildSingleEntityPath(final EntitySpec entity) {
    final String upperFirst = toUpperFirst(entity.getName());
    final String aspectParameterName = upperFirst + ASPECTS;
    final PathItem result = new PathItem();

    // Get Operation
    final List<Parameter> parameters =
        List.of(
            new Parameter()
                .in(NAME_PATH)
                .name("urn")
                .description("The entity's unique URN id.")
                .schema(new Schema().type(TYPE_STRING)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SYSTEM_METADATA)
                .description("Include systemMetadata with response.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .$ref(
                    String.format(
                        "#/components/parameters/%s", aspectParameterName + MODEL_VERSION)));
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
                                            upperFirst, ENTITY_RESPONSE_SUFFIX)))));
    final Operation getOperation =
        new Operation()
            .summary(String.format("Get %s.", upperFirst))
            .parameters(parameters)
            .tags(List.of(entity.getName() + " Entity"))
            .responses(new ApiResponses().addApiResponse("200", successApiResponse));

    // Head Operation
    final ApiResponse successHeadResponse =
        new ApiResponse()
            .description(String.format("%s  exists.", entity.getName()))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final ApiResponse notFoundHeadResponse =
        new ApiResponse()
            .description(String.format("%s does not exist.", entity.getName()))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final Operation headOperation =
        new Operation()
            .summary(String.format("%s existence.", upperFirst))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_PATH)
                        .name("urn")
                        .description("The entity's unique URN id.")
                        .schema(new Schema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_INCLUDE_SOFT_DELETE)
                        .description("If enabled, soft deleted items will exist.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false))))
            .tags(List.of(entity.getName() + " Entity"))
            .responses(
                new ApiResponses()
                    .addApiResponse("204", successHeadResponse)
                    .addApiResponse("404", notFoundHeadResponse));
    // Delete Operation
    final ApiResponse successDeleteResponse =
        new ApiResponse()
            .description(String.format("Delete %s entity.", upperFirst))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final Operation deleteOperation =
        new Operation()
            .summary(String.format("Delete entity %s", upperFirst))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_PATH)
                        .name("urn")
                        .description("The entity's unique URN id.")
                        .schema(new Schema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("clear")
                        .description("Delete all aspects, preserving the entity's key aspect.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .$ref(
                            String.format(
                                "#/components/parameters/%s",
                                aspectParameterName + MODEL_VERSION))))
            .tags(List.of(entity.getName() + " Entity"))
            .responses(new ApiResponses().addApiResponse("200", successDeleteResponse));

    return result.get(getOperation).head(headOperation).delete(deleteOperation);
  }

  private static PathItem buildListEntityPath(final EntitySpec entity) {
    final String upperFirst = toUpperFirst(entity.getName());
    final String aspectParameterName = upperFirst + ASPECTS;
    final PathItem result = new PathItem();
    final List<Parameter> parameters =
        List.of(
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SYSTEM_METADATA)
                .description("Include systemMetadata with response.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_INCLUDE_SOFT_DELETE)
                .description("Include soft-deleted aspects with response.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SKIP_CACHE)
                .description("Skip cache when listing entities.")
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
            .summary(String.format("Scroll/List %s.", upperFirst))
            .parameters(parameters)
            .tags(List.of(entity.getName() + " Entity"))
            .description(
                "Scroll indexed entities. Will not include soft deleted entities by default.")
            .responses(new ApiResponses().addApiResponse("200", successApiResponse)));

    // Post Operation
    final Content requestCreateContent =
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
    final ApiResponse apiCreateResponse =
        new ApiResponse()
            .description("Create a batch of " + entity.getName() + " entities.")
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
    final ApiResponse apiCreateAsyncResponse =
        new ApiResponse()
            .description("Async batch creation of " + entity.getName() + " entities submitted.")
            .content(new Content().addMediaType("application/json", new MediaType()));

    result.setPost(
        new Operation()
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("async")
                        .description("Use async ingestion for high throughput.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(true)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false))))
            .summary("Create " + upperFirst + " entities.")
            .tags(List.of(entity.getName() + " Entity"))
            .requestBody(
                new RequestBody()
                    .description("Create " + entity.getName() + " entities.")
                    .required(true)
                    .content(requestCreateContent))
            .responses(
                new ApiResponses()
                    .addApiResponse("200", apiCreateResponse)
                    .addApiResponse("202", apiCreateAsyncResponse)));

    return result;
  }

  private static PathItem buildBatchGetEntityPath(final EntitySpec entity) {
    final String upperFirst = toUpperFirst(entity.getName());
    final PathItem result = new PathItem();
    // Post Operation
    final Content requestBatch =
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
                                            "BatchGet" + upperFirst, ENTITY_REQUEST_SUFFIX)))));
    final ApiResponse apiBatchGetResponse =
        new ApiResponse()
            .description("Get a batch of " + entity.getName() + " entities.")
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
            .summary("Batch Get " + upperFirst + " entities.")
            .tags(List.of(entity.getName() + " Entity"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false))))
            .requestBody(
                new RequestBody()
                    .description("Batch Get " + entity.getName() + " entities.")
                    .required(true)
                    .content(requestBatch))
            .responses(new ApiResponses().addApiResponse("200", apiBatchGetResponse)));

    return result;
  }

  private static PathItem buildGenericListEntitiesPath() {
    final PathItem result = new PathItem();
    final List<Parameter> parameters =
        List.of(
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SYSTEM_METADATA)
                .description("Include systemMetadata with response.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_INCLUDE_SOFT_DELETE)
                .description("Include soft-deleted aspects with response.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SKIP_CACHE)
                .description("Skip cache when listing entities.")
                .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_PIT_KEEP_ALIVE)
                .description(
                    "Point In Time keep alive, accepts a time based string like \"5m\" for five minutes.")
                .schema(new Schema().type(TYPE_STRING)._default("5m")),
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
                                            ENTITIES, ENTITY_RESPONSE_SUFFIX)))));

    final RequestBody requestBody =
        new RequestBody()
            .description(
                "Scroll entities and aspects. If the `aspects` list is not specified then NO aspects will be returned. If the `aspects` list is emtpy, all aspects will be returned.")
            .required(false)
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
                                            ENTITIES, ENTITY_REQUEST_SUFFIX)))));

    result.setPost(
        new Operation()
            .summary(String.format("Scroll/List %s.", ENTITIES))
            .parameters(parameters)
            .tags(List.of("Generic Entities"))
            .description("Scroll indexed entities. Will not include soft deleted entities.")
            .requestBody(requestBody)
            .responses(new ApiResponses().addApiResponse("200", successApiResponse)));

    return result;
  }

  private static void addExtraParameters(final Components components) {
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
            .name("sortCriteria")
            .explode(true)
            .description("Sort fields for pagination.")
            .example(PROPERTY_URN)
            .schema(
                new Schema()
                    .type(TYPE_ARRAY)
                    ._default(List.of(PROPERTY_URN))
                    .items(new Schema<>().type(TYPE_STRING)._default(PROPERTY_URN))));
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
            .example(10)
            .schema(new Schema().type(TYPE_INTEGER)._default(10).minimum(new BigDecimal(1))));
    components.addParameters(
        "ScrollQuery" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_QUERY)
            .description(
                "Structured search query. See Elasticsearch documentation on `query_string` syntax.")
            .example("*")
            .schema(new Schema().type(TYPE_STRING)._default("*")));
  }

  private static Parameter buildParameterSchema(
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
            .items(
                new Schema()
                    .type(TYPE_STRING)
                    ._enum(aspectNames.stream().sorted().toList())
                    ._default(aspectNames.stream().findFirst().orElse(null)));
    return new Parameter()
        .in(NAME_QUERY)
        .name("aspects")
        .explode(true)
        .description("Aspects to include.")
        .required(false)
        .example(aspectNames)
        .schema(schema);
  }

  private static void addAspectSchemas(final Components components, final AspectSpec aspect) {
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
                  // Set enums to "string".
                  if (s.getEnum() != null && !s.getEnum().isEmpty()) {
                    s.setType("string");
                  } else {
                    Set<String> requiredNames =
                        Optional.ofNullable(s.getRequired())
                            .map(names -> Set.copyOf(names))
                            .orElse(new HashSet());
                    Map<String, Schema> properties =
                        Optional.ofNullable(s.getProperties()).orElse(new HashMap<>());
                    properties.forEach(
                        (name, schema) -> {
                          String $ref = schema.get$ref();
                          boolean isNameRequired = requiredNames.contains(name);
                          if ($ref != null && !isNameRequired) {
                            // A non-required $ref property must be wrapped in a { anyOf: [ $ref ] }
                            // object to allow the
                            // property to be marked as nullable
                            schema.setType(TYPE_OBJECT);
                            schema.set$ref(null);
                            schema.setAnyOf(List.of(new Schema().$ref($ref)));
                          }
                          schema.setNullable(!isNameRequired);
                        });
                  }
                  components.addSchemas(n, s);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate schema for cross-entity scroll/list response
   *
   * @param entityRegistry entity registry
   * @return schema
   */
  private static Schema buildAspectsRefResponseSchema(final EntityRegistry entityRegistry) {
    final Schema result =
        new Schema<>()
            .type(TYPE_OBJECT)
            .description(ASPECT_DESCRIPTION)
            .required(List.of(PROPERTY_VALUE));

    entityRegistry
        .getAspectSpecs()
        .values()
        .forEach(
            aspect ->
                result.addProperty(
                    PROPERTY_VALUE, new Schema<>().$ref(PATH_DEFINITIONS + aspect.getName())));
    result.addProperty(
        NAME_SYSTEM_METADATA,
        new Schema<>()
            .type(TYPE_OBJECT)
            .anyOf(List.of(new Schema().$ref(PATH_DEFINITIONS + "SystemMetadata")))
            .description("System metadata for the aspect.")
            .nullable(true));
    result.addProperty(
        NAME_AUDIT_STAMP,
        new Schema<>()
            .type(TYPE_OBJECT)
            .anyOf(List.of(new Schema().$ref(PATH_DEFINITIONS + "AuditStamp")))
            .description("Audit stamp for the aspect.")
            .nullable(true));
    return result;
  }

  private static Schema buildAspectRefResponseSchema(final String aspectName) {
    final Schema result =
        new Schema<>()
            .type(TYPE_OBJECT)
            .description(ASPECT_DESCRIPTION)
            .required(List.of(PROPERTY_VALUE))
            .addProperty(PROPERTY_VALUE, new Schema<>().$ref(PATH_DEFINITIONS + aspectName));
    result.addProperty(
        NAME_SYSTEM_METADATA,
        new Schema<>()
            .type(TYPE_OBJECT)
            .anyOf(List.of(new Schema().$ref(PATH_DEFINITIONS + "SystemMetadata")))
            .description("System metadata for the aspect.")
            .nullable(true));
    result.addProperty(
        NAME_AUDIT_STAMP,
        new Schema<>()
            .type(TYPE_OBJECT)
            .anyOf(List.of(new Schema().$ref(PATH_DEFINITIONS + "AuditStamp")))
            .description("Audit stamp for the aspect.")
            .nullable(true));
    return result;
  }

  private static Schema buildAspectRefRequestSchema(final String aspectName) {
    final Schema result =
        new Schema<>()
            .type(TYPE_OBJECT)
            .description(ASPECT_DESCRIPTION)
            .required(List.of(PROPERTY_VALUE))
            .addProperty(
                PROPERTY_VALUE, new Schema<>().$ref(PATH_DEFINITIONS + toUpperFirst(aspectName)));
    result.addProperty(
        NAME_SYSTEM_METADATA,
        new Schema<>()
            .type(TYPE_OBJECT)
            .anyOf(List.of(new Schema().$ref(PATH_DEFINITIONS + "SystemMetadata")))
            .description("System metadata for the aspect.")
            .nullable(true));

    Schema stringTypeSchema = new Schema<>();
    stringTypeSchema.setType(TYPE_STRING);
    result.addProperty(
        "headers",
        new Schema<>()
            .type(TYPE_OBJECT)
            .additionalProperties(stringTypeSchema)
            .description("System headers for the operation.")
            .nullable(true));
    return result;
  }

  private static Schema buildEntitySchema(
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

  /**
   * Generate cross-entity schema
   *
   * @param entityRegistry entity registry
   * @param withSystemMetadata include system metadata
   * @return schema
   */
  private static Schema buildEntitySchema(
      final EntityRegistry entityRegistry,
      final Set<String> aspectNames,
      final boolean withSystemMetadata) {
    final Map<String, Schema> properties =
        entityRegistry.getAspectSpecs().entrySet().stream()
            .filter(a -> aspectNames.contains(a.getValue().getName()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    a ->
                        buildAspectRef(
                            a.getValue().getPegasusSchema().getName(), withSystemMetadata)));
    properties.put(
        PROPERTY_URN, new Schema<>().type(TYPE_STRING).description("Unique id for " + ENTITIES));

    return new Schema<>()
        .type(TYPE_OBJECT)
        .description(ENTITIES + " object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  /**
   * Generate cross-entity schema
   *
   * @param entityRegistry entity registry
   * @param definitionNames include aspects
   * @return schema
   */
  private static Schema buildEntitiesRequestSchema(
      final EntityRegistry entityRegistry, final Set<String> definitionNames) {

    final Set<String> keyAspects = new HashSet<>();

    final List<String> entityNames =
        entityRegistry.getEntitySpecs().values().stream()
            .peek(entitySpec -> keyAspects.add(entitySpec.getKeyAspectName()))
            .map(EntitySpec::getName)
            .sorted()
            .toList();

    Schema entitiesSchema =
        new Schema().type(TYPE_ARRAY).items(new Schema().type(TYPE_STRING)._enum(entityNames));

    final List<String> aspectNames =
        entityRegistry.getAspectSpecs().values().stream()
            .filter(aspectSpec -> !aspectSpec.isTimeseries())
            .map(AspectSpec::getName)
            .filter(definitionNames::contains) // Only if aspect is defined
            .distinct()
            .sorted()
            .collect(Collectors.toList());

    Schema aspectsSchema =
        new Schema().type(TYPE_ARRAY).items(new Schema().type(TYPE_STRING)._enum(aspectNames));

    return new Schema<>()
        .type(TYPE_OBJECT)
        .description(ENTITIES + " request object.")
        .example(
            Map.of(
                "entities", entityNames.stream().filter(n -> !n.startsWith("dataHub")).toList(),
                "aspects",
                    aspectNames.stream()
                        .filter(n -> !n.startsWith("dataHub") && !keyAspects.contains(n))
                        .toList()))
        .properties(
            Map.of(
                "entities", entitiesSchema,
                "aspects", aspectsSchema));
  }

  /**
   * Generate schema for cross-entity scroll/list response
   *
   * @return schema
   */
  private static Schema buildEntitiesScrollSchema() {
    return new Schema<>()
        .type(TYPE_OBJECT)
        .description("Scroll across (list) " + ENTITIES + " objects.")
        .required(List.of("entities"))
        .addProperty(
            NAME_SCROLL_ID,
            new Schema<>().type(TYPE_STRING).description("Scroll id for pagination."))
        .addProperty(
            "entities",
            new Schema<>()
                .type(TYPE_ARRAY)
                .description(ENTITIES + " object.")
                .items(
                    new Schema<>()
                        .$ref(
                            String.format(
                                "#/components/schemas/%s%s", ENTITIES, ENTITY_RESPONSE_SUFFIX))));
  }

  private static Schema buildEntityScrollSchema(final EntitySpec entity) {
    return new Schema<>()
        .type(TYPE_OBJECT)
        .description("Scroll across (list) " + toUpperFirst(entity.getName()) + " objects.")
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

  private static Schema buildEntityBatchGetRequestSchema(
      final EntitySpec entity, Set<String> aspectNames) {

    final Map<String, Schema> properties =
        entity.getAspectSpecMap().entrySet().stream()
            .filter(a -> aspectNames.contains(a.getValue().getName()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    a -> new Schema().$ref("#/components/schemas/BatchGetRequestBody")));
    properties.put(
        PROPERTY_URN,
        new Schema<>().type(TYPE_STRING).description("Unique id for " + entity.getName()));

    properties.put(
        entity.getKeyAspectName(), new Schema().$ref("#/components/schemas/BatchGetRequestBody"));

    return new Schema<>()
        .type(TYPE_OBJECT)
        .description(toUpperFirst(entity.getName()) + " object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  private static Schema buildAspectRef(final String aspect, final boolean withSystemMetadata) {
    // A non-required $ref property must be wrapped in a { anyOf: [ $ref ] }
    // object to allow the
    // property to be marked as nullable
    final Schema result = new Schema<>();

    result.setType(TYPE_OBJECT);
    result.set$ref(null);
    result.setNullable(true);
    final String internalRef;
    if (withSystemMetadata) {
      internalRef =
          String.format(FORMAT_PATH_DEFINITIONS, toUpperFirst(aspect), ASPECT_RESPONSE_SUFFIX);
    } else {
      internalRef =
          String.format(FORMAT_PATH_DEFINITIONS, toUpperFirst(aspect), ASPECT_REQUEST_SUFFIX);
    }
    result.setAnyOf(List.of(new Schema().$ref(internalRef)));
    return result;
  }

  private static Schema buildAspectPatchSchema() {
    Map<String, Schema> properties =
        ImmutableMap.<String, Schema>builder()
            .put(
                PROPERTY_PATCH,
                new Schema<>()
                    .type(TYPE_ARRAY)
                    .items(
                        new Schema<>()
                            .type(TYPE_OBJECT)
                            .required(List.of("op", "path"))
                            .properties(
                                Map.of(
                                    "op", new Schema<>().type(TYPE_STRING),
                                    "path", new Schema<>().type(TYPE_STRING),
                                    "value", new Schema<>().type(TYPE_OBJECT)))))
            .put(PROPERTY_PATCH_PKEY, new Schema<>().type(TYPE_OBJECT))
            .build();

    return new Schema<>()
        .type(TYPE_OBJECT)
        .description(
            "Extended JSON Patch to allow for manipulating array sets which represent maps where each element has a unique primary key.")
        .required(List.of(PROPERTY_PATCH))
        .properties(properties);
  }

  private static PathItem buildSingleEntityAspectPath(
      final EntitySpec entity, final AspectSpec aspectSpec) {
    final String upperFirstEntity = toUpperFirst(entity.getName());

    List<String> tags = List.of(aspectSpec.getName() + " Aspect");
    // Get Operation
    final Parameter getParameter =
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_SYSTEM_METADATA)
            .description("Include systemMetadata with response.")
            .schema(new Schema().type(TYPE_BOOLEAN)._default(false));
    final Parameter versionParameter =
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_VERSION)
            .description(
                aspectSpec.isTimeseries()
                    ? "This aspect is a `timeseries` aspect, version=0 indicates the most recent aspect should be return. Otherwise return the most recent <= to version as epoch milliseconds."
                    : "Return a specific aspect version of the aspect.")
            .schema(new Schema().type(TYPE_INTEGER)._default(0).minimum(BigDecimal.ZERO));
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
                                            aspectSpec.getPegasusSchema().getName(),
                                            ASPECT_RESPONSE_SUFFIX)))));
    final Operation getOperation =
        new Operation()
            .summary(String.format("Get %s for %s.", aspectSpec.getName(), entity.getName()))
            .tags(tags)
            .parameters(List.of(getParameter, versionParameter))
            .responses(new ApiResponses().addApiResponse("200", successApiResponse));
    // Head Operation
    final ApiResponse successHeadResponse =
        new ApiResponse()
            .description(String.format("%s on %s exists.", aspectSpec.getName(), entity.getName()))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final ApiResponse notFoundHeadResponse =
        new ApiResponse()
            .description(
                String.format("%s on %s does not exist.", aspectSpec.getName(), entity.getName()))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final Operation headOperation =
        new Operation()
            .summary(String.format("%s on %s existence.", aspectSpec.getName(), upperFirstEntity))
            .tags(tags)
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_INCLUDE_SOFT_DELETE)
                        .description("If enabled, soft deleted items will exist.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false))))
            .responses(
                new ApiResponses()
                    .addApiResponse("200", successHeadResponse)
                    .addApiResponse("404", notFoundHeadResponse));
    // Delete Operation
    final ApiResponse successDeleteResponse =
        new ApiResponse()
            .description(
                String.format("Delete %s on %s entity.", aspectSpec.getName(), upperFirstEntity))
            .content(new Content().addMediaType("application/json", new MediaType()));
    final Operation deleteOperation =
        new Operation()
            .summary(
                String.format("Delete %s on entity %s", aspectSpec.getName(), upperFirstEntity))
            .tags(tags)
            .responses(new ApiResponses().addApiResponse("200", successDeleteResponse));
    // Post Operation
    final ApiResponse successPostResponse =
        new ApiResponse()
            .description(
                String.format(
                    "Create aspect %s on %s entity.", aspectSpec.getName(), upperFirstEntity))
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
                                            aspectSpec.getPegasusSchema().getName(),
                                            ASPECT_RESPONSE_SUFFIX)))));
    final RequestBody requestBody =
        new RequestBody()
            .description(
                String.format(
                    "Create aspect %s on %s entity.", aspectSpec.getName(), upperFirstEntity))
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
                                            aspectSpec.getPegasusSchema().getName(),
                                            ASPECT_REQUEST_SUFFIX)))));
    final Operation postOperation =
        new Operation()
            .summary(
                String.format("Create aspect %s on %s ", aspectSpec.getName(), upperFirstEntity))
            .tags(tags)
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("async")
                        .description("Use async ingestion for high throughput.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("createIfEntityNotExists")
                        .description("Only create the aspect if the Entity doesn't exist.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("createIfNotExists")
                        .description("Only create the aspect if the Aspect doesn't exist.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(true))))
            .requestBody(requestBody)
            .responses(new ApiResponses().addApiResponse("201", successPostResponse));
    // Patch Operation
    final ApiResponse successPatchResponse =
        new ApiResponse()
            .description(
                String.format(
                    "Patch aspect %s on %s entity.", aspectSpec.getName(), upperFirstEntity))
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
                                            aspectSpec.getPegasusSchema().getName(),
                                            ASPECT_RESPONSE_SUFFIX)))));
    final RequestBody patchRequestBody =
        new RequestBody()
            .description(
                String.format(
                    "Patch aspect %s on %s entity.", aspectSpec.getName(), upperFirstEntity))
            .required(true)
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(new Schema().$ref("#/components/schemas/AspectPatch"))));
    final Operation patchOperation =
        new Operation()
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(new Schema().type(TYPE_BOOLEAN)._default(false))))
            .summary(
                String.format("Patch aspect %s on %s ", aspectSpec.getName(), upperFirstEntity))
            .tags(tags)
            .requestBody(patchRequestBody)
            .responses(new ApiResponses().addApiResponse("200", successPatchResponse));
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
        .post(postOperation)
        .patch(patchOperation);
  }

  private static Schema buildVersionPropertiesRequestSchema() {
    return new Schema<>()
        .type(TYPE_OBJECT)
        .description("Properties for creating a version relationship")
        .properties(
            Map.of(
                "comment",
                    new Schema<>()
                        .type(TYPE_STRING)
                        .description("Comment about the version")
                        .nullable(true),
                "label",
                    new Schema<>()
                        .type(TYPE_STRING)
                        .description("Label for the version")
                        .nullable(true),
                "sourceCreationTimestamp",
                    new Schema<>()
                        .type(TYPE_INTEGER)
                        .description("Timestamp when version was created in source system")
                        .nullable(true),
                "sourceCreator",
                    new Schema<>()
                        .type(TYPE_STRING)
                        .description("Creator of version in source system")
                        .nullable(true)));
  }

  private static PathItem buildVersioningRelationshipPath() {
    final PathItem result = new PathItem();

    // Common parameters for path
    final List<Parameter> parameters =
        List.of(
            new Parameter()
                .in(NAME_PATH)
                .name("versionSetUrn")
                .description("The Version Set URN to unlink from")
                .required(true)
                .schema(new Schema().type(TYPE_STRING)),
            new Parameter()
                .in(NAME_PATH)
                .name("entityUrn")
                .description("The Entity URN to be unlinked")
                .required(true)
                .schema(new Schema().type(TYPE_STRING)));

    // Success response for DELETE
    final ApiResponse successDeleteResponse =
        new ApiResponse()
            .description("Successfully unlinked entity from version set")
            .content(new Content().addMediaType("application/json", new MediaType()));

    // DELETE operation
    final Operation deleteOperation =
        new Operation()
            .summary("Unlink an entity from a version set")
            .description("Removes the version relationship between an entity and a version set")
            .tags(List.of("Version Relationships"))
            .parameters(parameters)
            .responses(
                new ApiResponses()
                    .addApiResponse("200", successDeleteResponse)
                    .addApiResponse(
                        "404", new ApiResponse().description("Version Set or Entity not found")));

    // Success response for POST
    final ApiResponse successPostResponse =
        new ApiResponse()
            .description("Successfully linked entity to version set")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema<>()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            toUpperFirst(VERSION_PROPERTIES_ASPECT_NAME),
                                            ASPECT_RESPONSE_SUFFIX)))));

    // Request body for POST
    final RequestBody requestBody =
        new RequestBody()
            .description("Version properties for the link operation")
            .required(true)
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType().schema(buildVersionPropertiesRequestSchema())));

    // POST operation
    final Operation postOperation =
        new Operation()
            .summary("Link an entity to a version set")
            .description("Creates a version relationship between an entity and a version set")
            .tags(List.of("Version Relationships"))
            .parameters(parameters)
            .requestBody(requestBody)
            .responses(
                new ApiResponses()
                    .addApiResponse("201", successPostResponse)
                    .addApiResponse(
                        "404", new ApiResponse().description("Version Set or Entity not found")));

    return result.delete(deleteOperation).post(postOperation);
  }
}
