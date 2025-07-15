package io.datahubproject.openapi.v3;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static com.linkedin.metadata.aspect.patch.GenericJsonPatch.ARRAY_PRIMARY_KEYS_FIELD;
import static io.datahubproject.openapi.util.ReflectionCache.toUpperFirst;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.processing.ProcessingUtil;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.avro.SchemaTranslator;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@SuppressWarnings({"rawtypes", "unchecked"})
public class OpenAPIV3Generator {
  private static final SpecVersion SPEC_VERSION = SpecVersion.V31;

  private static final String PATH_PREFIX = "/openapi/v3";
  private static final String MODEL_VERSION = "_v3";
  private static final String TYPE_OBJECT = "object";
  private static final String TYPE_BOOLEAN = "boolean";
  private static final String TYPE_STRING = "string";
  private static final String TYPE_ARRAY = "array";
  private static final String TYPE_INTEGER = "integer";
  private static final String TYPE_NULL = "null";
  private static final Set<String> TYPE_OBJECT_NULLABLE = Set.of(TYPE_OBJECT, TYPE_NULL);
  private static final Set<String> TYPE_STRING_NULLABLE = Set.of(TYPE_STRING, TYPE_NULL);
  private static final Set<String> TYPE_INTEGER_NULLABLE = Set.of(TYPE_INTEGER, TYPE_NULL);
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
  private static final String PATH_DEFINITIONS = "#/components/schemas/";
  private static final String FORMAT_PATH_DEFINITIONS = "#/components/schemas/%s%s";
  private static final String ASPECT_DESCRIPTION = "Aspect wrapper object.";
  private static final String REQUEST_SUFFIX = "Request" + MODEL_VERSION;
  private static final String RESPONSE_SUFFIX = "Response" + MODEL_VERSION;
  private static final String REQUEST_PATCH_SUFFIX = "RequestPatch" + MODEL_VERSION;
  private static final String ASPECT_PATCH = "AspectPatch";

  private static final String ASPECT_REQUEST_SUFFIX = "Aspect" + REQUEST_SUFFIX;
  private static final String ASPECT_RESPONSE_SUFFIX = "Aspect" + RESPONSE_SUFFIX;
  private static final String ENTITY_REQUEST_SUFFIX = "Entity" + REQUEST_SUFFIX;
  private static final String ENTITY_RESPONSE_SUFFIX = "Entity" + RESPONSE_SUFFIX;
  private static final String ENTITY_REQUEST_PATCH_SUFFIX = "Entity" + REQUEST_PATCH_SUFFIX;
  private static final String NAME_SKIP_CACHE = "skipCache";
  private static final String ASPECTS = "Aspects";
  private static final String ENTITIES = "Entities";

  private static final String CROSS_ENTITIES = "CrossEntities";
  private static final String CROSS_ENTITY_REQUEST_SUFFIX = "Request" + MODEL_VERSION;
  private static final String CROSS_ENTITY_PATCH_SUFFIX = "Patch" + MODEL_VERSION;
  private static final String CROSS_ENTITY_RESPONSE_SUFFIX = "Response" + MODEL_VERSION;
  private static final String CROSS_ENTITY_BATCHGET_SUFFIX = "BatchGetRequest" + MODEL_VERSION;

  private static final Set<String> EXCLUDE_ENTITIES = Set.of("dataHubOpenAPISchema");
  private static final Set<String> EXCLUDE_ASPECTS = Set.of("dataHubOpenAPISchemaKey");
  private static final String ASPECT_PATCH_PROPERTY = "AspectPatchProperty";

  public static OpenAPI generateOpenApiSpec(
      EntityRegistry entityRegistry, ConfigurationProvider configurationProvider) {

    final Map<String, EntitySpec> filteredEntitySpec = getEntitySpecs(entityRegistry);
    final Map<String, AspectSpec> filteredAspectSpec = getAspectSpecs(entityRegistry);

    final Set<String> entityNames = filteredEntitySpec.keySet();
    final Set<String> aspectNames = filteredAspectSpec.keySet();

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
        ENTITIES + ENTITY_REQUEST_SUFFIX,
        buildEntitiesRequestSchema(filteredEntitySpec, filteredAspectSpec, aspectNames));
    components.addSchemas(
        ENTITIES + ENTITY_RESPONSE_SUFFIX,
        buildEntitySchema(filteredAspectSpec, aspectNames, true));
    components.addSchemas(
        "Scroll" + ENTITIES + ENTITY_RESPONSE_SUFFIX, buildEntitiesScrollSchema());
    components.addSchemas(ASPECT_PATCH_PROPERTY, buildAspectPatchPropertySchema());

    // --> Aspect components
    components.addSchemas(ASPECT_PATCH, buildAspectPatchSchema());
    components.addSchemas(
        "BatchGetRequestBody",
        newSchema()
            .types(TYPE_OBJECT_NULLABLE)
            .description("Request body for batch get aspects.")
            .properties(
                Map.of(
                    "headers",
                    newSchema()
                        .types(TYPE_OBJECT_NULLABLE)
                        .additionalProperties(newSchema().type(TYPE_STRING))
                        .description("System headers for the operation."))));

    // --> Aspect components
    filteredAspectSpec
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
        filteredEntitySpec.values().stream()
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
          components.addSchemas(
              entityName + ENTITY_REQUEST_PATCH_SUFFIX,
              buildEntityPatchSchema(e, aspectNames, true));
        });

    components.addSchemas(
        "SortOrder", newSchema().type(TYPE_STRING)._enum(List.of("ASCENDING", "DESCENDING")));
    components.addSchemas(
        ENTITIES + ENTITY_REQUEST_PATCH_SUFFIX,
        buildEntitiesPatchRequestSchema(definedEntitySpecs));
    components.addSchemas(
        CROSS_ENTITIES + CROSS_ENTITY_BATCHGET_SUFFIX,
        buildCrossEntityBatchGetRequestSchema(definedEntitySpecs));
    components.addSchemas(
        CROSS_ENTITIES + CROSS_ENTITY_REQUEST_SUFFIX,
        buildCrossEntityUpsertSchema(definedEntitySpecs));
    components.addSchemas(
        CROSS_ENTITIES + CROSS_ENTITY_PATCH_SUFFIX,
        buildCrossEntityPatchSchema(definedEntitySpecs));
    components.addSchemas(
        CROSS_ENTITIES + CROSS_ENTITY_RESPONSE_SUFFIX,
        buildCrossEntityResponseSchema(definedEntitySpecs));

    // Parameters

    // --> Entity Parameters
    definedEntitySpecs.forEach(
        e -> {
          final String parameterName = toUpperFirst(e.getName()) + ASPECTS;
          components.addParameters(
              parameterName + MODEL_VERSION, buildParameterSchema(e, definitionNames));
        });

    addExtraParameters(
        configurationProvider.getSearchService().getLimit().getResults(), components);

    // ----- Generic-entity parameter ------------------------------------------
    Set<String> unionAspectNames = filteredAspectSpec.keySet(); // all aspects
    components.addParameters(
        ENTITIES + ASPECTS + MODEL_VERSION, buildGenericAspectsParameter(unionAspectNames));

    // Path
    final Paths paths = new Paths();

    // --> Cross-entity Paths
    paths.addPathItem(PATH_PREFIX + "/entity/scroll", buildGenericListEntitiesPath());

    // --> Entity Paths
    definedEntitySpecs.forEach(
        e -> {
          paths.addPathItem(
              String.format(PATH_PREFIX + "/entity/%s", e.getName().toLowerCase()),
              buildListEntityPath(e));
          paths.addPathItem(
              String.format(PATH_PREFIX + "/entity/%s/batchGet", e.getName().toLowerCase()),
              buildBatchGetEntityPath(e));
          paths.addPathItem(
              String.format(PATH_PREFIX + "/entity/%s/{urn}", e.getName().toLowerCase()),
              buildSingleEntityPath(e));
        });

    // ----------  Generic Entity paths  --------------------------------------
    paths.addPathItem(PATH_PREFIX + "/entity/generic", buildListGenericEntitiesPath());
    paths.addPathItem(PATH_PREFIX + "/entity/generic/batchGet", buildBatchGetGenericEntitiesPath());
    paths.addPathItem(PATH_PREFIX + "/entity/generic/{urn}", buildSingleGenericEntityPath());

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
                                PATH_PREFIX + "/entity/%s/{urn}/%s",
                                e.getName().toLowerCase(),
                                a.getName().toLowerCase()),
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
                                PATH_PREFIX + "/entity/%s/{urn}/%s",
                                e.getName().toLowerCase(),
                                a.getName().toLowerCase()),
                            buildSingleEntityAspectPath(e, a))));

    // --> Link & Unlink APIs
    if (configurationProvider.getFeatureFlags().isEntityVersioning()) {
      definedEntitySpecs.stream()
          .filter(entitySpec -> VERSION_SET_ENTITY_NAME.equals(entitySpec.getName()))
          .forEach(
              entitySpec -> {
                paths.addPathItem(
                    PATH_PREFIX
                        + "/entity/versioning/{versionSetUrn}/relationship/versionOf/{entityUrn}",
                    buildVersioningRelationshipPath());
              });
    }

    return new OpenAPI(SPEC_VERSION)
        .openapi("3.1.0")
        .info(info)
        .paths(paths)
        .components(components);
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
                .schema(newSchema().type(TYPE_STRING)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SYSTEM_METADATA)
                .description("Include systemMetadata with response.")
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
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
                                newSchema()
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
                        .schema(newSchema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_INCLUDE_SOFT_DELETE)
                        .description("If enabled, soft deleted items will exist.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
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
                        .schema(newSchema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("clear")
                        .description("Delete all aspects, preserving the entity's key aspect.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .$ref(
                            String.format(
                                "#/components/parameters/%s",
                                aspectParameterName + MODEL_VERSION))))
            .tags(List.of(entity.getName() + " Entity"))
            .responses(new ApiResponses().addApiResponse("200", successDeleteResponse));

    return result.get(getOperation).head(headOperation).delete(deleteOperation);
  }

  /**
   * Builds Entity paths for a given entity which involves a list of entities
   *
   * @param entity the entity spec to build for
   * @return path for /entity/{entityName}
   */
  private static PathItem buildListEntityPath(final EntitySpec entity) {
    final String upperFirst = toUpperFirst(entity.getName());
    final String aspectParameterName = upperFirst + ASPECTS;
    final PathItem result = new PathItem();

    // Get/Scroll/List
    final List<Parameter> parameters =
        List.of(
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SYSTEM_METADATA)
                .description("Include systemMetadata with response.")
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_INCLUDE_SOFT_DELETE)
                .description("Include soft-deleted aspects with response.")
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SKIP_CACHE)
                .description("Skip cache when listing entities.")
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .$ref(
                    String.format(
                        "#/components/parameters/%s", aspectParameterName + MODEL_VERSION)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_PIT_KEEP_ALIVE)
                .description(
                    "Point In Time keep alive, accepts a time based string like \"5m\" for five minutes.")
                .schema(newSchema().type(TYPE_STRING)._default("5m")),
            new Parameter().$ref("#/components/parameters/PaginationCount" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/ScrollId" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/SortBy" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/SortOrder" + MODEL_VERSION),
            new Parameter().$ref("#/components/parameters/ScrollQuery" + MODEL_VERSION));
    final ApiResponse getApiResponse =
        new ApiResponse()
            .description("Success")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
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
            .responses(new ApiResponses().addApiResponse("200", getApiResponse)));

    // Post Operation
    final Content createRequestContent =
        new Content()
            .addMediaType(
                "application/json",
                new MediaType()
                    .schema(
                        newSchema()
                            .type(TYPE_ARRAY)
                            .items(
                                newSchema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            upperFirst, ENTITY_REQUEST_SUFFIX)))));
    final ApiResponse createAPIResponse =
        new ApiResponse()
            .description("Create a batch of " + entity.getName() + " entities.")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
                                    .type(TYPE_ARRAY)
                                    .items(
                                        newSchema()
                                            .$ref(
                                                String.format(
                                                    "#/components/schemas/%s%s",
                                                    upperFirst, ENTITY_RESPONSE_SUFFIX))))));
    final ApiResponse createAPIAsyncResponse =
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
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(true)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
            .summary("Create " + upperFirst + " entities.")
            .tags(List.of(entity.getName() + " Entity"))
            .requestBody(
                new RequestBody()
                    .description("Create " + entity.getName() + " entities.")
                    .required(true)
                    .content(createRequestContent))
            .responses(
                new ApiResponses()
                    .addApiResponse("200", createAPIResponse)
                    .addApiResponse("202", createAPIAsyncResponse)));

    // Patch Operation
    final Content patchRequestContent =
        new Content()
            .addMediaType(
                "application/json",
                new MediaType()
                    .schema(
                        newSchema()
                            .type(TYPE_ARRAY)
                            .items(
                                newSchema()
                                    .$ref(
                                        String.format(
                                            "#/components/schemas/%s%s",
                                            upperFirst, ENTITY_REQUEST_PATCH_SUFFIX)))));
    final ApiResponse patchAPIResponse =
        new ApiResponse()
            .description("Patch a batch of " + entity.getName() + " entities.")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
                                    .type(TYPE_ARRAY)
                                    .items(
                                        newSchema()
                                            .$ref(
                                                String.format(
                                                    "#/components/schemas/%s%s",
                                                    upperFirst, ENTITY_RESPONSE_SUFFIX))))));
    final ApiResponse patchAPIAsyncResponse =
        new ApiResponse()
            .description("Async batch patch of " + entity.getName() + " entities submitted.")
            .content(new Content().addMediaType("application/json", new MediaType()));

    result.setPatch(
        new Operation()
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("async")
                        .description("Use async ingestion for high throughput.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(true)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
            .summary("Patch " + upperFirst + " entities.")
            .tags(List.of(entity.getName() + " Entity"))
            .requestBody(
                new RequestBody()
                    .description("Patch " + entity.getName() + " entities.")
                    .required(true)
                    .content(patchRequestContent))
            .responses(
                new ApiResponses()
                    .addApiResponse("200", patchAPIResponse)
                    .addApiResponse("202", patchAPIAsyncResponse)));

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
                        newSchema()
                            .type(TYPE_ARRAY)
                            .items(
                                newSchema()
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
                                newSchema()
                                    .type(TYPE_ARRAY)
                                    .items(
                                        newSchema()
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
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
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
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_INCLUDE_SOFT_DELETE)
                .description("Include soft-deleted aspects with response.")
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_SKIP_CACHE)
                .description("Skip cache when listing entities.")
                .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
            new Parameter()
                .in(NAME_QUERY)
                .name(NAME_PIT_KEEP_ALIVE)
                .description(
                    "Point In Time keep alive, accepts a time based string like \"5m\" for five minutes.")
                .schema(newSchema().types(TYPE_STRING_NULLABLE)._default("5m")),
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
                                newSchema()
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
                                newSchema()
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

  /* =============================================================== */
  /*  /openapi/v3/entity      (GET | POST | PATCH)                  */
  /* =============================================================== */
  private static PathItem buildListGenericEntitiesPath() {
    /* ---------- POST (create) ----------------------------------- */
    Content createBodyContent =
        new Content()
            .addMediaType(
                "application/json",
                new MediaType()
                    .schema(
                        newSchema()
                            .$ref(
                                "#/components/schemas/"
                                    + CROSS_ENTITIES
                                    + CROSS_ENTITY_REQUEST_SUFFIX)));

    ApiResponse post200 =
        new ApiResponse()
            .description("Created entities")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
                                    .$ref(
                                        "#/components/schemas/"
                                            + CROSS_ENTITIES
                                            + CROSS_ENTITY_RESPONSE_SUFFIX))));
    ApiResponse post202 =
        new ApiResponse()
            .description("Async batch creation submitted")
            .content(new Content().addMediaType("application/json", new MediaType()));
    Operation postOp =
        new Operation()
            .summary("Create Generic Entities.")
            .tags(List.of("Generic Entities"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("async")
                        .description("Use async ingestion for high throughput.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(true)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
            .requestBody(
                new RequestBody()
                    .required(true)
                    .description("Generic entity upsert.")
                    .content(createBodyContent))
            .responses(
                new ApiResponses().addApiResponse("200", post200).addApiResponse("202", post202));

    /* ---------- PATCH (generic json-patch) ----------------------- */
    Content patchBodyContent =
        new Content()
            .addMediaType(
                "application/json",
                new MediaType()
                    .schema(
                        newSchema()
                            .$ref(
                                "#/components/schemas/"
                                    + CROSS_ENTITIES
                                    + CROSS_ENTITY_PATCH_SUFFIX)));

    ApiResponse patch200 =
        new ApiResponse()
            .description("Patched entities")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
                                    .$ref(
                                        "#/components/schemas/"
                                            + CROSS_ENTITIES
                                            + CROSS_ENTITY_RESPONSE_SUFFIX))));
    ApiResponse patch202 =
        new ApiResponse()
            .description("Async batch patch submitted")
            .content(new Content().addMediaType("application/json", new MediaType()));
    Operation patchOp =
        new Operation()
            .summary("Patch Generic Entities.")
            .tags(List.of("Generic Entities"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("async")
                        .description("Use async ingestion for high throughput.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(true)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
            .requestBody(
                new RequestBody()
                    .required(true)
                    .description("Generic entity patch.")
                    .content(patchBodyContent))
            .responses(
                new ApiResponses().addApiResponse("200", patch200).addApiResponse("202", patch202));

    return new PathItem().post(postOp).patch(patchOp);
  }

  /* =============================================================== */
  /*  /openapi/v3/entity/batchGet   (POST)                          */
  /* =============================================================== */
  private static PathItem buildBatchGetGenericEntitiesPath() {
    Content body =
        new Content()
            .addMediaType(
                "application/json",
                new MediaType()
                    .schema(
                        newSchema()
                            .$ref(
                                "#/components/schemas/"
                                    + CROSS_ENTITIES
                                    + CROSS_ENTITY_BATCHGET_SUFFIX)));

    ApiResponse ok =
        new ApiResponse()
            .description("Batch result")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
                                    .$ref(
                                        "#/components/schemas/"
                                            + CROSS_ENTITIES
                                            + CROSS_ENTITY_RESPONSE_SUFFIX))));

    Operation op =
        new Operation()
            .summary("Batch Get Generic Entities.")
            .tags(List.of("Generic Entities"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
            .requestBody(new RequestBody().required(true).content(body))
            .responses(new ApiResponses().addApiResponse("200", ok));

    return new PathItem().post(op);
  }

  /* =============================================================== */
  /*  /openapi/v3/entity/{urn}  (GET | HEAD | DELETE)               */
  /* =============================================================== */
  private static PathItem buildSingleGenericEntityPath() {
    String aspectParamRef =
        String.format("#/components/parameters/%s", ENTITIES + ASPECTS + MODEL_VERSION);

    /* ---------- GET --------------------------------------------- */
    ApiResponse ok =
        new ApiResponse()
            .description("Success")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
                                    .$ref(
                                        "#/components/schemas/"
                                            + ENTITIES
                                            + ENTITY_RESPONSE_SUFFIX))));
    Operation getOp =
        new Operation()
            .summary("Get Generic Entity.")
            .tags(List.of("Generic Entities"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_PATH)
                        .name("urn")
                        .required(true)
                        .description("Entity URN")
                        .schema(newSchema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter().$ref(aspectParamRef)))
            .responses(new ApiResponses().addApiResponse("200", ok));

    /* ---------- HEAD -------------------------------------------- */
    ApiResponse head204 =
        new ApiResponse()
            .description("Entity exists.")
            .content(new Content().addMediaType("application/json", new MediaType()));
    ApiResponse head404 =
        new ApiResponse()
            .description("Entity not found.")
            .content(new Content().addMediaType("application/json", new MediaType()));
    Operation headOp =
        new Operation()
            .summary("Generic Entity existence.")
            .tags(List.of("Generic Entities"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_PATH)
                        .name("urn")
                        .required(true)
                        .schema(newSchema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_INCLUDE_SOFT_DELETE)
                        .description("Include soft-deleted items.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
            .responses(
                new ApiResponses().addApiResponse("204", head204).addApiResponse("404", head404));

    /* ---------- DELETE ------------------------------------------ */
    ApiResponse delOk =
        new ApiResponse()
            .description("Entity deleted.")
            .content(new Content().addMediaType("application/json", new MediaType()));
    Operation delOp =
        new Operation()
            .summary("Delete Generic Entity.")
            .tags(List.of("Generic Entities"))
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_PATH)
                        .name("urn")
                        .required(true)
                        .schema(newSchema().type(TYPE_STRING)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("clear")
                        .description("Delete all aspects, preserve key aspect.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter().$ref(aspectParamRef)))
            .responses(new ApiResponses().addApiResponse("200", delOk));

    return new PathItem()
        .parameters(
            List.of(
                new Parameter()
                    .in(NAME_PATH)
                    .name("urn")
                    .required(true)
                    .schema(newSchema().type(TYPE_STRING))))
        .get(getOp)
        .head(headOp)
        .delete(delOp);
  }

  private static Parameter buildGenericAspectsParameter(Set<String> aspectNames) {
    Schema schema =
        newSchema()
            .type(TYPE_ARRAY)
            .items(
                newSchema()
                    .type(TYPE_STRING)
                    ._enum(aspectNames.stream().sorted().toList())
                    ._default(null));
    return new Parameter()
        .in(NAME_QUERY)
        .name("aspects")
        .explode(true)
        .description("Aspects to include.")
        .required(false)
        .schema(schema);
  }

  private static void addExtraParameters(
      ResultsLimitConfig searchResultsLimit, final Components components) {
    components.addParameters(
        "ScrollId" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_SCROLL_ID)
            .description("Scroll pagination token.")
            .schema(newSchema().type(TYPE_STRING)));
    components.addParameters(
        "SortBy" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name("sortCriteria")
            .explode(true)
            .description("Sort fields for pagination.")
            .example(List.of(PROPERTY_URN))
            .schema(
                newSchema()
                    .type(TYPE_ARRAY)
                    ._default(List.of(PROPERTY_URN))
                    .items(newSchema().type(TYPE_STRING)._default(PROPERTY_URN))));
    components.addParameters(
        "SortOrder" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name("sortOrder")
            .explode(true)
            .description("Sort direction field for pagination.")
            .example("ASCENDING")
            .schema(newSchema()._default("ASCENDING").$ref("#/components/schemas/SortOrder")));
    components.addParameters(
        "PaginationCount" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name("count")
            .description("Number of items per page.")
            .example(10)
            .schema(
                newSchema()
                    .type(TYPE_INTEGER)
                    ._default(searchResultsLimit.getApiDefault())
                    .maximum(BigDecimal.valueOf(searchResultsLimit.getMax()))
                    .minimum(new BigDecimal(1))));
    components.addParameters(
        "ScrollQuery" + MODEL_VERSION,
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_QUERY)
            .description(
                "Structured search query. See Elasticsearch documentation on `query_string` syntax.")
            .example("*")
            .schema(newSchema().type(TYPE_STRING)._default("*")));
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
        newSchema()
            .type(TYPE_ARRAY)
            .items(
                newSchema()
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
                  s.specVersion(SPEC_VERSION);

                  // Set enums to "string".
                  if (s.getEnum() != null && !s.getEnum().isEmpty()) {
                    if (s.getNullable() != null && s.getNullable()) {
                      nullableSchema(s, TYPE_STRING_NULLABLE);
                    } else {
                      s.setType(TYPE_STRING);
                    }
                  } else {
                    Set<String> requiredNames =
                        Optional.ofNullable(s.getRequired())
                            .map(names -> Set.copyOf(names))
                            .orElse(new HashSet());

                    Map<String, Schema> properties =
                        Optional.ofNullable(s.getProperties()).orElse(new HashMap<>());
                    properties.forEach(
                        (name, schema) -> {
                          schema.specVersion(SpecVersion.V31);
                          String $ref = schema.get$ref();

                          boolean isNameRequired = requiredNames.contains(name);

                          if (definitions.has(n)) {
                            JsonNode field = definitions.get(n);
                            boolean hasDefault =
                                field.get("properties").get(name).has("default")
                                    && !field.get("properties").get(name).get("default").isNull();
                            if (hasDefault) {
                              // A default value means it is not required, regardless of nullability
                              s.getRequired().remove(name);
                              if (s.getRequired().isEmpty()) {
                                s.setRequired(null);
                              }
                            }
                          }

                          if ($ref != null && !isNameRequired) {
                            // A non-required $ref property must be wrapped in a { oneOf: [ $ref,
                            // null ] }
                            // object to allow the
                            // property to be marked as nullable
                            schema.setType(null);
                            schema.set$ref(null);
                            schema.setOneOf(
                                List.of(newSchema().$ref($ref), newSchema().type(TYPE_NULL)));
                          }

                          if ($ref == null) {
                            if (schema.getEnum() != null && !schema.getEnum().isEmpty()) {
                              if ((schema.getNullable() != null && schema.getNullable())
                                  || !isNameRequired) {
                                nullableSchema(schema, TYPE_STRING_NULLABLE);
                              } else {
                                schema.setType(TYPE_STRING);
                              }
                            } else if (schema.getEnum() == null && !isNameRequired) {
                              nullableSchema(schema, Set.of(schema.getType(), TYPE_NULL));
                            }
                          }
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

  private static Schema buildAspectRefResponseSchema(final String aspectName) {
    final Schema result =
        newSchema()
            .type(TYPE_OBJECT)
            .description(ASPECT_DESCRIPTION)
            .required(List.of(PROPERTY_VALUE))
            .addProperty(PROPERTY_VALUE, newSchema().$ref(PATH_DEFINITIONS + aspectName));
    result.addProperty(
        NAME_SYSTEM_METADATA,
        newSchema()
            .types(TYPE_OBJECT_NULLABLE)
            .oneOf(
                List.of(
                    newSchema().$ref(PATH_DEFINITIONS + "SystemMetadata"),
                    newSchema().type(TYPE_NULL)))
            .description("System metadata for the aspect."));
    result.addProperty(
        NAME_AUDIT_STAMP,
        newSchema()
            .types(TYPE_OBJECT_NULLABLE)
            .oneOf(
                List.of(
                    newSchema().$ref(PATH_DEFINITIONS + "AuditStamp"), newSchema().type(TYPE_NULL)))
            .description("Audit stamp for the aspect."));
    return result;
  }

  private static Schema buildAspectRefRequestSchema(final String aspectName) {
    final Schema result =
        newSchema()
            .type(TYPE_OBJECT)
            .description(ASPECT_DESCRIPTION)
            .required(List.of(PROPERTY_VALUE))
            .addProperty(
                PROPERTY_VALUE, newSchema().$ref(PATH_DEFINITIONS + toUpperFirst(aspectName)));
    result.addProperty(
        NAME_SYSTEM_METADATA,
        newSchema()
            .types(TYPE_OBJECT_NULLABLE)
            .oneOf(
                List.of(
                    newSchema().$ref(PATH_DEFINITIONS + "SystemMetadata"),
                    newSchema().type(TYPE_NULL)))
            .description("System metadata for the aspect."));

    Schema stringTypeSchema = newSchema();
    stringTypeSchema.setType(TYPE_STRING);
    result.addProperty(
        "headers",
        newSchema()
            .types(TYPE_OBJECT_NULLABLE)
            .additionalProperties(stringTypeSchema)
            .description("System headers for the operation."));
    return result;
  }

  private static Schema buildEntitySchema(
      final EntitySpec entity, Set<String> aspectNames, final boolean withSystemMetadata) {
    final Map<String, Schema> properties = new LinkedHashMap<>();
    properties.put(
        PROPERTY_URN,
        newSchema().type(TYPE_STRING).description("Unique id for " + entity.getName()));

    final Map<String, Schema> aspectProperties =
        entity.getAspectSpecMap().entrySet().stream()
            .filter(a -> aspectNames.contains(a.getValue().getName()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    a ->
                        buildAspectRef(
                            a.getValue().getPegasusSchema().getName(), withSystemMetadata)));
    properties.putAll(aspectProperties);

    properties.put(
        entity.getKeyAspectName(),
        buildAspectRef(entity.getKeyAspectSpec().getPegasusSchema().getName(), withSystemMetadata));

    return newSchema()
        .type(TYPE_OBJECT)
        .description(toUpperFirst(entity.getName()) + " object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  /**
   * Build a schema for multi-entity patching
   *
   * @return schema
   */
  private static Schema buildEntityPatchSchema(
      final EntitySpec entity, Set<String> aspectNames, final boolean withSystemMetadata) {
    final Map<String, Schema> patchProperties =
        entity.getAspectSpecMap().entrySet().stream()
            .filter(a -> aspectNames.contains(a.getValue().getName()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    a -> newSchema().$ref(PATH_DEFINITIONS + ASPECT_PATCH_PROPERTY)));

    final Map<String, Schema> properties = new LinkedHashMap<>();
    properties.put(
        PROPERTY_URN, newSchema().type(TYPE_STRING).description("Unique id for " + ENTITIES));
    properties.putAll(patchProperties);

    return newSchema()
        .type(TYPE_OBJECT)
        .description(ENTITIES + " object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  /**
   * Generate cross-entity schema
   *
   * @param aspectSpecs entity registry aspect specs
   * @param withSystemMetadata include system metadata
   * @return schema
   */
  private static Schema buildEntitySchema(
      final Map<String, AspectSpec> aspectSpecs,
      final Set<String> aspectNames,
      final boolean withSystemMetadata) {
    final Map<String, Schema> properties =
        aspectSpecs.entrySet().stream()
            .filter(a -> aspectNames.contains(a.getValue().getName()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    a ->
                        buildAspectRef(
                            a.getValue().getPegasusSchema().getName(), withSystemMetadata)));
    properties.put(
        PROPERTY_URN, newSchema().type(TYPE_STRING).description("Unique id for " + ENTITIES));

    return newSchema()
        .type(TYPE_OBJECT)
        .description(ENTITIES + " object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  /**
   * Generate cross-entity schema
   *
   * @param entitySpecs filtered map of entity name to entity spec
   * @param aspectSpecs filtered map of aspect name to aspect specs
   * @param definitionNames include aspects
   * @return schema
   */
  private static Schema buildEntitiesRequestSchema(
      final Map<String, EntitySpec> entitySpecs,
      final Map<String, AspectSpec> aspectSpecs,
      final Set<String> definitionNames) {

    final Set<String> keyAspects = new HashSet<>();

    final List<String> entityNames =
        entitySpecs.values().stream()
            .peek(entitySpec -> keyAspects.add(entitySpec.getKeyAspectName()))
            .map(EntitySpec::getName)
            .sorted()
            .toList();

    Schema entitiesSchema =
        newSchema().type(TYPE_ARRAY).items(newSchema().type(TYPE_STRING)._enum(entityNames));

    final List<String> aspectNames =
        aspectSpecs.values().stream()
            .filter(aspectSpec -> !aspectSpec.isTimeseries())
            .map(AspectSpec::getName)
            .filter(definitionNames::contains) // Only if aspect is defined
            .distinct()
            .sorted()
            .collect(Collectors.toList());

    Schema aspectsSchema =
        newSchema().type(TYPE_ARRAY).items(newSchema().type(TYPE_STRING)._enum(aspectNames));

    return newSchema()
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
                "entities", newSchema().oneOf(List.of(entitiesSchema, newSchema().type(TYPE_NULL))),
                "aspects", newSchema().oneOf(List.of(aspectsSchema, newSchema().type(TYPE_NULL)))));
  }

  private static Schema buildEntitiesPatchRequestSchema(List<EntitySpec> entitySpecs) {
    Map<String, Schema> props = new LinkedHashMap<>();

    entitySpecs.forEach(
        e ->
            props.put(
                e.getName(), // property name (lower-case entity name)
                newSchema()
                    .type(TYPE_ARRAY)
                    .items(
                        newSchema()
                            .$ref(
                                String.format(
                                    "#/components/schemas/%s%s",
                                    toUpperFirst(e.getName()), // <Entity>
                                    ENTITY_REQUEST_PATCH_SUFFIX)))));

    return newSchema()
        .type(TYPE_OBJECT)
        .description("Mixed-entity patch request body.")
        .additionalProperties(false)
        .properties(props);
  }

  /**
   * Generate schema for cross-entity scroll/list response
   *
   * @return schema
   */
  private static Schema buildEntitiesScrollSchema() {
    return newSchema()
        .type(TYPE_OBJECT)
        .description("Scroll across (list) " + ENTITIES + " objects.")
        .required(List.of("entities"))
        .addProperty(
            NAME_SCROLL_ID, newSchema().type(TYPE_STRING).description("Scroll id for pagination."))
        .addProperty(
            "entities",
            newSchema()
                .type(TYPE_ARRAY)
                .description(ENTITIES + " object.")
                .items(
                    newSchema()
                        .$ref(
                            String.format(
                                "#/components/schemas/%s%s", ENTITIES, ENTITY_RESPONSE_SUFFIX))));
  }

  private static Schema buildEntityScrollSchema(final EntitySpec entity) {
    return newSchema()
        .type(TYPE_OBJECT)
        .description("Scroll across (list) " + toUpperFirst(entity.getName()) + " objects.")
        .required(List.of("entities"))
        .addProperty(
            NAME_SCROLL_ID, newSchema().type(TYPE_STRING).description("Scroll id for pagination."))
        .addProperty(
            "entities",
            newSchema()
                .type(TYPE_ARRAY)
                .description(toUpperFirst(entity.getName()) + " object.")
                .items(
                    newSchema()
                        .$ref(
                            String.format(
                                "#/components/schemas/%s%s",
                                toUpperFirst(entity.getName()), ENTITY_RESPONSE_SUFFIX))));
  }

  private static Schema buildEntityBatchGetRequestSchema(
      final EntitySpec entity, Set<String> aspectNames) {

    Map<String, Schema> properties = new LinkedHashMap<>();
    properties.put(
        PROPERTY_URN,
        newSchema().type(TYPE_STRING).description("Unique id for " + entity.getName()));

    entity.getAspectSpecMap().entrySet().stream()
        .filter(
            e ->
                aspectNames.contains(e.getValue().getName())
                    || e.getKey().equals(entity.getKeyAspectName()))
        .forEach(
            e ->
                properties.put(
                    e.getKey(),
                    newSchema()
                        .types(TYPE_OBJECT_NULLABLE)
                        .oneOf(
                            List.of(
                                newSchema().$ref("#/components/schemas/BatchGetRequestBody"),
                                newSchema().type(TYPE_NULL)))));

    return newSchema()
        .type(TYPE_OBJECT)
        .description(toUpperFirst(entity.getName()) + "object.")
        .required(List.of(PROPERTY_URN))
        .properties(properties);
  }

  private static Schema buildCrossEntityUpsertSchema(List<EntitySpec> entitySpecs) {

    Map<String, Schema> props = new LinkedHashMap<>();

    entitySpecs.forEach(
        e -> {
          Schema arraySchema =
              newSchema()
                  .type(TYPE_ARRAY)
                  .items(
                      newSchema()
                          .$ref(
                              String.format(
                                  "#/components/schemas/%s%s",
                                  toUpperFirst(e.getName()), ENTITY_REQUEST_SUFFIX)));
          props.put(
              e.getName(), newSchema().oneOf(List.of(arraySchema, newSchema().type(TYPE_NULL))));
        });

    return newSchema()
        .type(TYPE_OBJECT)
        .description("Mixed-entity upsert request body.")
        .additionalProperties(false)
        .properties(props);
  }

  private static Schema buildCrossEntityPatchSchema(List<EntitySpec> entitySpecs) {

    Map<String, Schema> props = new LinkedHashMap<>();

    entitySpecs.forEach(
        e -> {
          Schema arraySchema =
              newSchema()
                  .type(TYPE_ARRAY)
                  .items(
                      newSchema()
                          .$ref(
                              String.format(
                                  "#/components/schemas/%s%s",
                                  toUpperFirst(e.getName()), ENTITY_REQUEST_PATCH_SUFFIX)));

          props.put(
              e.getName(), newSchema().oneOf(List.of(newSchema().type(TYPE_NULL), arraySchema)));
        });

    return newSchema()
        .type(TYPE_OBJECT)
        .description("Mixed-entity patch request body.")
        .additionalProperties(false)
        .properties(props);
  }

  private static Schema buildCrossEntityResponseSchema(List<EntitySpec> entitySpecs) {
    Map<String, Schema> props = new LinkedHashMap<>();

    entitySpecs.forEach(
        e -> {
          Schema arraySchema =
              newSchema()
                  .type(TYPE_ARRAY)
                  .items(
                      newSchema()
                          .$ref(
                              String.format(
                                  "#/components/schemas/%s%s",
                                  toUpperFirst(e.getName()), ENTITY_RESPONSE_SUFFIX)));

          props.put(
              e.getName(), newSchema().oneOf(List.of(arraySchema, newSchema().type(TYPE_NULL))));
        });

    return newSchema()
        .type(TYPE_OBJECT)
        .description("Mixed-entity upsert / patch response.")
        .additionalProperties(false)
        .properties(props);
  }

  private static Schema buildCrossEntityBatchGetRequestSchema(List<EntitySpec> entitySpecs) {

    Map<String, Schema> props = new LinkedHashMap<>();

    entitySpecs.forEach(
        e -> {
          Schema arraySchema =
              newSchema()
                  .type(TYPE_ARRAY)
                  .items(
                      newSchema()
                          .$ref(
                              String.format(
                                  "#/components/schemas/%s%s",
                                  "BatchGet" + toUpperFirst(e.getName()), ENTITY_REQUEST_SUFFIX)));

          props.put(
              e.getName(), newSchema().oneOf(List.of(arraySchema, newSchema().type(TYPE_NULL))));
        });

    return newSchema()
        .type(TYPE_OBJECT)
        .description("Mixed-entity batch-get request body.")
        .additionalProperties(false)
        .properties(props);
  }

  private static Schema buildAspectRef(final String aspect, final boolean withSystemMetadata) {
    final Schema result = newSchema();

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
    result.setOneOf(List.of(newSchema().$ref(internalRef), newSchema().type(TYPE_NULL)));
    return result;
  }

  private static Schema buildAspectPatchSchema() {
    Map<String, Schema> properties =
        ImmutableMap.<String, Schema>builder()
            .put(
                PROPERTY_PATCH,
                newSchema()
                    .type(TYPE_ARRAY)
                    .items(
                        newSchema()
                            .type(TYPE_OBJECT)
                            .required(List.of("op", "path"))
                            .additionalProperties(false)
                            .properties(
                                Map.of(
                                    "op",
                                        newSchema()
                                            .type(TYPE_STRING)
                                            .description("Operation type")
                                            ._enum(
                                                List.of(
                                                    "add", "remove", "replace", "move", "copy",
                                                    "test")),
                                    "path",
                                        newSchema()
                                            .type(TYPE_STRING)
                                            .description("JSON pointer to the target location")
                                            .format("json-pointer"),
                                    "from",
                                        newSchema()
                                            .type(TYPE_STRING)
                                            .description(
                                                "JSON pointer for source location (required for move/copy)"),
                                    "value",
                                        newSchema() // No type restriction to allow any JSON
                                            // value
                                            .description(
                                                "The value to use for this operation (if applicable)")))))
            .put(
                ARRAY_PRIMARY_KEYS_FIELD,
                newSchema()
                    .type(TYPE_OBJECT)
                    .description("Maps array paths to their primary key field names")
                    .additionalProperties(
                        newSchema().type(TYPE_ARRAY).items(newSchema().type(TYPE_STRING))))
            .put(
                "forceGenericPatch",
                newSchema()
                    .type(TYPE_BOOLEAN)
                    ._default(false)
                    .description(
                        "Flag to force using generic patching regardless of other conditions"))
            .build();

    return newSchema()
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
            .schema(newSchema().type(TYPE_BOOLEAN)._default(false));
    final Parameter versionParameter =
        new Parameter()
            .in(NAME_QUERY)
            .name(NAME_VERSION)
            .description(
                aspectSpec.isTimeseries()
                    ? "This aspect is a `timeseries` aspect, version=0 indicates the most recent aspect should be return. Otherwise return the most recent <= to version as epoch milliseconds."
                    : "Return a specific aspect version of the aspect.")
            .schema(newSchema().type(TYPE_INTEGER)._default(0).minimum(BigDecimal.ZERO));
    final ApiResponse successApiResponse =
        new ApiResponse()
            .description("Success")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                newSchema()
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
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
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
                                newSchema()
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
                                newSchema()
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
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("createIfEntityNotExists")
                        .description("Only create the aspect if the Entity doesn't exist.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false)),
                    new Parameter()
                        .in(NAME_QUERY)
                        .name("createIfNotExists")
                        .description("Only create the aspect if the Aspect doesn't exist.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(true))))
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
                                newSchema()
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
                            .schema(
                                newSchema()
                                    .$ref(
                                        String.format("#/components/schemas/%s", ASPECT_PATCH)))));
    final Operation patchOperation =
        new Operation()
            .parameters(
                List.of(
                    new Parameter()
                        .in(NAME_QUERY)
                        .name(NAME_SYSTEM_METADATA)
                        .description("Include systemMetadata with response.")
                        .schema(newSchema().type(TYPE_BOOLEAN)._default(false))))
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
                    .schema(newSchema().type(TYPE_STRING))))
        .get(getOperation)
        .head(headOperation)
        .delete(deleteOperation)
        .post(postOperation)
        .patch(patchOperation);
  }

  private static Schema buildVersionPropertiesRequestSchema() {
    return newSchema()
        .type(TYPE_OBJECT)
        .description("Properties for creating a version relationship")
        .properties(
            Map.of(
                "comment",
                    newSchema()
                        .types(TYPE_STRING_NULLABLE)
                        .description("Comment about the version"),
                "label",
                    newSchema().types(TYPE_STRING_NULLABLE).description("Label for the version"),
                "sourceCreationTimestamp",
                    newSchema()
                        .types(TYPE_INTEGER_NULLABLE)
                        .description("Timestamp when version was created in source system"),
                "sourceCreator",
                    newSchema()
                        .types(TYPE_STRING_NULLABLE)
                        .description("Creator of version in source system")));
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
                .schema(newSchema().type(TYPE_STRING)),
            new Parameter()
                .in(NAME_PATH)
                .name("entityUrn")
                .description("The Entity URN to be unlinked")
                .required(true)
                .schema(newSchema().type(TYPE_STRING)));

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
                                newSchema()
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

  private static Schema<?> buildAspectPatchPropertySchema() {
    Schema<?> schema = new Schema<>();
    schema.type(TYPE_OBJECT);
    schema.required(List.of(PROPERTY_VALUE));
    schema.addProperty(
        PROPERTY_VALUE,
        new Schema<>()
            .$ref(PATH_DEFINITIONS + ASPECT_PATCH)
            .description("Patch to apply to the aspect."));
    schema.addProperty(
        NAME_SYSTEM_METADATA,
        newSchema()
            .types(TYPE_OBJECT_NULLABLE)
            .oneOf(
                List.of(
                    newSchema().$ref(PATH_DEFINITIONS + "SystemMetadata"),
                    newSchema().type(TYPE_NULL)))
            .description("System metadata for the aspect."));
    schema.addProperty(
        "headers",
        new Schema<>()
            .types(Set.of(TYPE_OBJECT, "nullable"))
            .nullable(true)
            .additionalProperties(new Schema<>().type(TYPE_STRING))
            .description("System headers for the operation."));
    return schema;
  }

  private static Map<String, EntitySpec> getEntitySpecs(@Nonnull EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs().entrySet().stream()
        .filter(
            entry ->
                EXCLUDE_ENTITIES.stream()
                    .noneMatch(
                        excludeEntityName -> excludeEntityName.equalsIgnoreCase(entry.getKey())))
        .collect(Collectors.toMap(e -> e.getValue().getName(), Map.Entry::getValue));
  }

  private static Map<String, AspectSpec> getAspectSpecs(@Nonnull EntityRegistry entityRegistry) {
    return Collections.unmodifiableMap(
        entityRegistry.getAspectSpecs().entrySet().stream()
            .filter(
                entry ->
                    EXCLUDE_ASPECTS.stream()
                        .noneMatch(
                            excludeAspectName ->
                                excludeAspectName.equalsIgnoreCase(entry.getKey())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> existing)));
  }

  private static Schema newSchema() {
    return new Schema().specVersion(SPEC_VERSION);
  }

  private static Schema nullableSchema(Schema origin, Set<String> types) {
    if (origin == null) {
      return newSchema().types(types);
    }

    String nonNullType = types.stream().filter(t -> !"null".equals(t)).findFirst().orElse(null);

    origin.setType(nonNullType);
    origin.setTypes(types);

    return origin;
  }
}
