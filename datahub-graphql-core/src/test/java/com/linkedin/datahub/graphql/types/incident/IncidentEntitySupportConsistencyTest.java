package com.linkedin.datahub.graphql.types.incident;

import static com.linkedin.datahub.graphql.Constants.GMS_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.INCIDENTS_SCHEMA_FILE;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeUtil;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Fails the build when the three places that decide "does this entity support incidents?" disagree.
 *
 * <p>They are maintained separately and already drift:
 *
 * <ol>
 *   <li>write allowlist: {@code IncidentInfo.pdl}, the {@code IncidentOn} relationship's
 *       entityTypes. Decides whether raiseIncident accepts the URN at all.
 *   <li>summary and health: {@code entity-registry.yml}, entities declaring {@code
 *       incidentsSummary}. Decides whether the entity carries a rolled-up status.
 *   <li>read API: {@code incident.graphql}, entity types with an {@code incidents} field typed
 *       {@code EntityIncidentsResult}. Decides whether anything can list them back.
 * </ol>
 *
 * <p>#18478 widened the PDL list to include mlModel, mlFeature, service and aiAgent, added
 * incidentsSummary for service and aiAgent only, and wired GraphQL for none of them. The result on
 * v1.7.0 is that raiseIncident accepts an mlModel URN, stores the incident against the model, and
 * nothing can read it back. The caller gets an incident URN and no reason to think anything went
 * wrong, which is worse than a clean rejection. See #18999 and #18911.
 *
 * <p>All three sets are derived from the sources themselves, so adding an entity to one layer and
 * forgetting the other two fails here rather than in production. The read set is also checked
 * against {@code entity.graphql}: the type that carries the field must exist there and implement
 * {@code Entity}, because an {@code extend type SchemaField} parses cleanly, binds to the struct
 * inside a schema aspect rather than to the {@code SchemaFieldEntity} entity, and resolves nothing
 * in production.
 *
 * <p>An entity with no GraphQL object type at all (service and aiAgent today) cannot take the read
 * layer, so for it only the write and summary layers have to agree. That is the dash in the #19322
 * tracker legend rather than a gap. {@link #describeGap} carries the swap if such entities should
 * count as gaps instead.
 *
 * <p>{@link #KNOWN_GAPS} excuses entities knowingly supported in one layer and not the others. It
 * is empty since #18911 and #19115 closed the ML catalog and schemaField gaps, and the #19322
 * tracker asks that this test not merge with a populated allowlist; any future entry pins the shape
 * it is excused in and fails as soon as that shape changes.
 *
 * <p><b>Boundaries.</b> The tracker counts seven gates and this test derives the first three. Gate
 * 4, the resolver wiring list in {@code GmsGraphQLEngine}, and gate 7, the entity fragments in the
 * MCP server's {@code LIST_INCIDENTS_QUERY}, are deliberately not derived here yet. A type present
 * in the SDL but absent from the wiring list resolves through the default {@code
 * PropertyDataFetcher} and returns null with no error, so SDL agreement is necessary but not
 * sufficient. Both gates will be added once #19395 (resolvers wired from the SDL) and #19396 (MCP
 * query generated from the SDL) land, deriving from the SDL rather than from a hoisted constant or
 * a Python file read by repo path. Gates 5 and 6 are frontend surfaces, the per-entity badge alias
 * and the getEntityIncidents inline fragments, and out of reach of a Java test. #18685 derives the
 * same three backend layers at docs build time; this test is the stricter of the two and wins a
 * disagreement. Raised by @cnpierrepapi on #18999.
 */
public class IncidentEntitySupportConsistencyTest {

  private static final String INCIDENT_ENTITY = "incident";
  private static final String INCIDENT_INFO_ASPECT = "incidentInfo";
  private static final String INCIDENTS_SUMMARY_ASPECT = "incidentsSummary";
  private static final String INCIDENT_ON_RELATIONSHIP = "IncidentOn";
  private static final String INCIDENTS_FIELD = "incidents";
  private static final String INCIDENTS_RESULT_TYPE = "EntityIncidentsResult";
  private static final String ENTITY_INTERFACE = "Entity";
  private static final String ENTITY_REGISTRY_RESOURCE = "entity-registry.yml";

  /**
   * A few entity types are suffixed in GraphQL to avoid colliding with a non-entity type of the
   * same name: {@code schemaField} is {@code SchemaFieldEntity} because {@code SchemaField} is
   * already the struct inside a schema aspect.
   */
  private static final String GRAPHQL_ENTITY_SUFFIX = "Entity";

  /** Repo paths, used only to make the failure message actionable. */
  private static final String PDL_PATH =
      "metadata-models/src/main/pegasus/com/linkedin/incident/IncidentInfo.pdl";

  private static final String REGISTRY_PATH =
      "metadata-models/src/main/resources/entity-registry.yml";
  private static final String GRAPHQL_PATH =
      "datahub-graphql-core/src/main/resources/" + INCIDENTS_SCHEMA_FILE;
  private static final String ENTITY_GRAPHQL_PATH =
      "datahub-graphql-core/src/main/resources/" + GMS_SCHEMA_FILE;

  /**
   * The shape an entity is knowingly excused in: whether it is expected in each of the three
   * layers, and why. Pinning the shape rather than only the name means a regression on an excused
   * entity (incidentsSummary vanishing from it, say) still fails here, and so does partial progress
   * that leaves the stated reason stale.
   */
  private record KnownGap(boolean writable, boolean summarised, boolean readable, String reason) {

    private boolean matches(
        final boolean isWritable, final boolean isSummarised, final boolean isReadable) {
      return writable == isWritable && summarised == isSummarised && readable == isReadable;
    }
  }

  /**
   * Entities knowingly supported in one layer and not the others, as of this commit.
   *
   * <p>Empty since #18911 and #19115 closed the ML catalog and schemaField gaps, and meant to stay
   * that way: the #19322 tracker asks that this test not merge with a populated allowlist. Every
   * entry is a bug, not a design decision, and states the shape it is excused in, for example
   * {@code "mlModel", new KnownGap(true, false, false, "#NNNNN: writable, no incidentsSummary, no
   * GraphQL read")}. An entry whose shape no longer matches reality fails the test, whether the gap
   * regressed or closed.
   */
  private static final Map<String, KnownGap> KNOWN_GAPS = new TreeMap<>(Map.of());

  private EntityRegistry entityRegistry;

  @BeforeTest
  public void setup() {
    // Building the registry runs the Pegasus annotation processor, which trips its own assertions
    // under -ea. Same guard every other registry-backed test in the repo uses.
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);

    final InputStream registryStream =
        Snapshot.class.getClassLoader().getResourceAsStream(ENTITY_REGISTRY_RESOURCE);
    assertNotNull(
        registryStream,
        ENTITY_REGISTRY_RESOURCE
            + " is not on the test classpath; this test derives its sets from it");
    entityRegistry = new ConfigEntityRegistry(registryStream);
  }

  @Test
  public void incidentSupportIsConsistentAcrossAllThreeLayers() {
    final EntityRegistry registry = entityRegistry;
    final Map<String, String> entityNamesByLowerCase = indexEntityNames(registry);

    // Names the test could not map back to an entity, and GraphQL types that carry the incidents
    // field without being an entity type production would resolve it on. Distinct from drift: it
    // means the schema attached the field to the wrong type, or the test has gone stale against a
    // convention change, and neither must be reported as a passing schema.
    final List<String> unmapped = new ArrayList<>();

    final TypeDefinitionRegistry incidentSchema = parseSchema(INCIDENTS_SCHEMA_FILE);
    final TypeDefinitionRegistry graphQlSchema = parseSchema(GMS_SCHEMA_FILE);
    graphQlSchema.merge(incidentSchema);

    final Set<String> writable = writeAllowlist(registry, entityNamesByLowerCase, unmapped);
    final Set<String> summarised = entitiesDeclaringIncidentsSummary(registry);
    final Set<String> graphQlTyped = graphQlEntityTypes(graphQlSchema, entityNamesByLowerCase);
    final Set<String> readable =
        graphQlReadableEntities(incidentSchema, graphQlSchema, entityNamesByLowerCase, unmapped);

    assertTrue(
        unmapped.isEmpty(),
        "This test could not map every incident-bearing name back to an entity in "
            + REGISTRY_PATH
            + ", or a GraphQL type carries the incidents field without being an entity type.\n"
            + "Fix the schema, or teach the test the new name, rather than deleting the check.\n\n"
            + String.join("\n", unmapped)
            + "\n");

    // Guards against a source moving or a parse silently yielding nothing, which would make the
    // equality below vacuously true. An empty GraphQL entity set would instead make the read layer
    // not applicable to everything, which is the same vacuous pass by another route.
    assertTrue(
        writable.size() > 1,
        "derived an implausible write allowlist from " + PDL_PATH + ": " + writable);
    assertTrue(
        readable.size() > 1,
        "derived an implausible GraphQL read set from " + GRAPHQL_PATH + ": " + readable);
    assertTrue(
        graphQlTyped.size() > 1,
        "derived an implausible set of GraphQL entity types from "
            + ENTITY_GRAPHQL_PATH
            + ": "
            + graphQlTyped);

    final Set<String> mentionedAnywhere = new TreeSet<>(writable);
    mentionedAnywhere.addAll(summarised);
    mentionedAnywhere.addAll(readable);

    final List<String> problems =
        mentionedAnywhere.stream()
            .filter(entity -> !KNOWN_GAPS.containsKey(entity))
            .map(entity -> describeGap(entity, writable, summarised, readable, graphQlTyped))
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(ArrayList::new));

    // An excused entity must still be in the shape it was excused in, and that shape must still be
    // a gap, or the allowlist stops meaning anything.
    KNOWN_GAPS.forEach(
        (entity, gap) -> {
          final boolean isWritable = writable.contains(entity);
          final boolean isSummarised = summarised.contains(entity);
          final boolean isReadable = readable.contains(entity);
          if (!gap.matches(isWritable, isSummarised, isReadable)) {
            problems.add(
                "  `"
                    + entity
                    + "` is excused in KNOWN_GAPS as "
                    + describeShape(gap.writable(), gap.summarised(), gap.readable())
                    + " but is now "
                    + describeShape(isWritable, isSummarised, isReadable)
                    + ". Update or remove the entry, which currently reads: "
                    + gap.reason());
            return;
          }
          if (describeGap(entity, writable, summarised, readable, graphQlTyped) != null) {
            return;
          }
          final String state =
              mentionedAnywhere.contains(entity)
                  ? "is consistent across all applicable layers now"
                  : "is no longer referenced by any of the three layers";
          problems.add(
              "  `"
                  + entity
                  + "` "
                  + state
                  + ". Remove it from KNOWN_GAPS, where it is currently excused as: "
                  + gap.reason());
        });

    assertTrue(
        problems.isEmpty(),
        "Incident entity support has drifted between its three sources of truth.\n"
            + "These must be changed together, or raiseIncident accepts writes that nothing\n"
            + "can read back.\n\n"
            + "  write allowlist   "
            + PDL_PATH
            + " (IncidentOn entityTypes)\n"
            + "  summary / health  "
            + REGISTRY_PATH
            + " (incidentsSummary aspect)\n"
            + "  read API          "
            + GRAPHQL_PATH
            + " (incidents field typed EntityIncidentsResult)\n\n"
            + String.join("\n", problems)
            + "\n");
  }

  /**
   * One line per inconsistent type, naming the layer that is missing.
   *
   * <p>Phrased as an instruction rather than as a set difference on purpose. Three people hit three
   * different rejection wordings for this condition and none of them named the layer that was
   * missing, which is what sent all three digging through the registry by hand.
   *
   * <p>An entity with no GraphQL object type cannot carry an incidents field, so the read layer
   * does not apply to it and only the write and summary layers have to agree: the dash in the
   * #19322 tracker legend rather than a gap. service and aiAgent are in that state today. To count
   * such entities as gaps instead, drop {@code hasGraphQlType} from the short-circuit below, turn
   * the closing note into a {@code missing} entry, and excuse each of them in {@link #KNOWN_GAPS}
   * with the shape it is in.
   *
   * @return null when the entity is consistent across all layers that apply to it
   */
  private static String describeGap(
      final String entity,
      final Set<String> writable,
      final Set<String> summarised,
      final Set<String> readable,
      final Set<String> graphQlTyped) {
    final boolean isWritable = writable.contains(entity);
    final boolean isSummarised = summarised.contains(entity);
    final boolean isReadable = readable.contains(entity);
    final boolean hasGraphQlType = graphQlTyped.contains(entity);
    if (isWritable == isSummarised && (isSummarised == isReadable || !hasGraphQlType)) {
      return null;
    }

    final Set<String> present = new LinkedHashSet<>();
    final Set<String> missing = new LinkedHashSet<>();
    if (isWritable) {
      present.add("is an IncidentOn destination in " + PDL_PATH);
    } else {
      missing.add("the IncidentOn entityTypes list in " + PDL_PATH);
    }
    if (isSummarised) {
      present.add("declares the incidentsSummary aspect in " + REGISTRY_PATH);
    } else {
      missing.add("the incidentsSummary aspect in " + REGISTRY_PATH);
    }
    if (isReadable) {
      present.add("exposes an incidents field in " + GRAPHQL_PATH);
    } else if (hasGraphQlType) {
      missing.add("an incidents field on its GraphQL type in " + GRAPHQL_PATH);
    }
    String note = "";
    if (!hasGraphQlType) {
      note = " (no GraphQL type in " + ENTITY_GRAPHQL_PATH + ", so the read layer does not apply)";
    }
    return "  `"
        + entity
        + "` "
        + String.join(" and ", present)
        + ", but is missing "
        + String.join(" and ", missing)
        + note;
  }

  /** The (writable, summarised, readable) triple in the words the failure messages use. */
  private static String describeShape(
      final boolean isWritable, final boolean isSummarised, final boolean isReadable) {
    return String.format(
        "(writable=%s, summarised=%s, readable=%s)", isWritable, isSummarised, isReadable);
  }

  /** Valid destinations of the IncidentOn relationship, off the incidentInfo aspect spec. */
  private static Set<String> writeAllowlist(
      final EntityRegistry registry,
      final Map<String, String> entityNamesByLowerCase,
      final List<String> unmapped) {
    final EntitySpec incident = registry.getEntitySpec(INCIDENT_ENTITY);
    return incident.getAspectSpec(INCIDENT_INFO_ASPECT).getRelationshipFieldSpecs().stream()
        .filter(spec -> INCIDENT_ON_RELATIONSHIP.equals(spec.getRelationshipName()))
        .flatMap(spec -> spec.getValidDestinationTypes().stream())
        .map(
            destination ->
                resolveEntity(
                    destination,
                    entityNamesByLowerCase,
                    unmapped,
                    "  `"
                        + destination
                        + "` is an IncidentOn destination in "
                        + PDL_PATH
                        + " but is not an entity in "
                        + REGISTRY_PATH))
        .filter(Objects::nonNull)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /** Entities whose aspect list includes incidentsSummary. */
  private static Set<String> entitiesDeclaringIncidentsSummary(final EntityRegistry registry) {
    return registry.getEntitySpecs().values().stream()
        .filter(spec -> Boolean.TRUE.equals(spec.hasAspect(INCIDENTS_SUMMARY_ASPECT)))
        .map(EntitySpec::getName)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * Loads and parses one schema file through the same {@link GmsGraphQLEngine#fileBasedSchema} call
   * and the same {@link SchemaParser} the engine itself uses, so this reads the schema the way
   * production does rather than pattern-matching the file. It stops short of {@code
   * makeExecutableSchema}: an assembled schema object needs every other SDL file plus the full
   * runtime wiring (services and resolvers), which is a heavier fixture than a schema-consistency
   * check should require, and it would not answer a different question.
   *
   * <p>The resource is checked before the loader runs because the loader reports a missing file as
   * a bare NullPointerException out of IOUtils rather than as a message naming the file.
   */
  private static TypeDefinitionRegistry parseSchema(final String fileName) {
    assertNotNull(
        Thread.currentThread().getContextClassLoader().getResource(fileName),
        fileName + " is not on the test classpath; this test derives its read set from it");
    return new SchemaParser().parse(GmsGraphQLEngine.fileBasedSchema(fileName));
  }

  /**
   * Registry entities that have a GraphQL object type at all: a type listing {@code Entity} in its
   * implements whose name maps back to a registry entity. Decides whether the read layer applies to
   * an entity. Names that do not map are dropped here rather than reported: nothing hangs on an
   * Entity type the registry does not know, and one that carries the incidents field is caught by
   * {@link #graphQlReadableEntities} instead.
   */
  private static Set<String> graphQlEntityTypes(
      final TypeDefinitionRegistry graphQlSchema,
      final Map<String, String> entityNamesByLowerCase) {
    return graphQlSchema.getTypes(ObjectTypeDefinition.class).stream()
        .filter(IncidentEntitySupportConsistencyTest::implementsEntity)
        .map(type -> lookupEntity(type.getName(), entityNamesByLowerCase))
        .filter(Objects::nonNull)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * Entities whose GraphQL type carries the incidents field, as production resolves it.
   *
   * <p>A type is a candidate only when its {@code incidents} field is typed {@code
   * EntityIncidentsResult}, wrappers aside, not merely because a field has that name. Two further
   * checks then apply and each failure is reported through {@code unmapped} rather than skipped:
   * the type must be an object type that exists once entity.graphql is merged in, and it must list
   * {@code Entity} in its implements. An {@code extend type SchemaField} parses cleanly, binds to
   * the struct inside a schema aspect rather than to {@code SchemaFieldEntity}, and resolves
   * nothing in production, so it must not count as read support.
   *
   * <p>{@code extend type X} is the shape incident.graphql uses to attach the field. Plain {@code
   * type X} declarations in this file are incident-domain types (Incident, EntityIncidentsResult)
   * and go through the same checks, so one of them counts only when it is an entity in its own
   * right.
   */
  private static Set<String> graphQlReadableEntities(
      final TypeDefinitionRegistry incidentSchema,
      final TypeDefinitionRegistry graphQlSchema,
      final Map<String, String> entityNamesByLowerCase,
      final List<String> unmapped) {
    final Set<String> readable = new TreeSet<>();

    incidentSchema
        .objectTypeExtensions()
        .forEach(
            (typeName, extensions) -> {
              if (extensions.stream()
                  .noneMatch(IncidentEntitySupportConsistencyTest::hasIncidents)) {
                return;
              }
              final String entity =
                  readableEntity(typeName, graphQlSchema, entityNamesByLowerCase, unmapped);
              if (entity != null) {
                readable.add(entity);
              }
            });

    incidentSchema.getTypes(ObjectTypeDefinition.class).stream()
        .filter(IncidentEntitySupportConsistencyTest::hasIncidents)
        .map(
            type -> readableEntity(type.getName(), graphQlSchema, entityNamesByLowerCase, unmapped))
        .filter(Objects::nonNull)
        .forEach(readable::add);

    return readable;
  }

  /**
   * Maps a type that carries the incidents field onto its entity, after checking that it is a type
   * production would resolve the field on. Reports through {@code unmapped} and returns null
   * otherwise.
   */
  private static String readableEntity(
      final String typeName,
      final TypeDefinitionRegistry graphQlSchema,
      final Map<String, String> entityNamesByLowerCase,
      final List<String> unmapped) {
    final ObjectTypeDefinition type =
        graphQlSchema.getType(typeName, ObjectTypeDefinition.class).orElse(null);
    if (type == null) {
      unmapped.add(
          "  GraphQL type `"
              + typeName
              + "` carries the incidents field in "
              + GRAPHQL_PATH
              + " but is not an object type declared in "
              + ENTITY_GRAPHQL_PATH
              + " or "
              + GRAPHQL_PATH
              + "; if it is declared in another schema file, load that file here too");
      return null;
    }
    if (!implementsEntity(type)) {
      unmapped.add(
          "  GraphQL type `"
              + typeName
              + "` carries the incidents field in "
              + GRAPHQL_PATH
              + " but does not implement Entity in "
              + ENTITY_GRAPHQL_PATH
              + ", so the field sits on a non-entity type and nothing resolves it");
      return null;
    }
    return resolveEntity(
        typeName,
        entityNamesByLowerCase,
        unmapped,
        "  GraphQL type `"
            + typeName
            + "` in "
            + GRAPHQL_PATH
            + " has an incidents field but does not map to an entity in "
            + REGISTRY_PATH);
  }

  /** True when the type has an incidents field typed EntityIncidentsResult, wrappers aside. */
  private static boolean hasIncidents(final ObjectTypeDefinition type) {
    return type.getFieldDefinitions().stream()
        .filter(field -> INCIDENTS_FIELD.equals(field.getName()))
        .map(field -> TypeUtil.unwrapAll(field.getType()).getName())
        .anyMatch(INCIDENTS_RESULT_TYPE::equals);
  }

  private static boolean implementsEntity(final ObjectTypeDefinition type) {
    return type.getImplements().stream()
        .map(TypeUtil::unwrapAll)
        .map(TypeName::getName)
        .anyMatch(ENTITY_INTERFACE::equals);
  }

  /**
   * Maps a GraphQL type name or a PDL destination type onto the entity name the registry uses,
   * recording the failure instead of silently dropping it.
   */
  private static String resolveEntity(
      final String name,
      final Map<String, String> entityNamesByLowerCase,
      final List<String> unmapped,
      final String failureMessage) {
    final String entity = lookupEntity(name, entityNamesByLowerCase);
    if (entity == null) {
      unmapped.add(failureMessage);
    }
    return entity;
  }

  /** The lookup behind {@link #resolveEntity}, for callers that do not want a miss reported. */
  private static String lookupEntity(
      final String name, final Map<String, String> entityNamesByLowerCase) {
    final String direct = entityNamesByLowerCase.get(name.toLowerCase(Locale.ROOT));
    if (direct != null) {
      return direct;
    }
    if (name.endsWith(GRAPHQL_ENTITY_SUFFIX)) {
      final String stripped =
          name.substring(0, name.length() - GRAPHQL_ENTITY_SUFFIX.length())
              .toLowerCase(Locale.ROOT);
      return entityNamesByLowerCase.get(stripped);
    }
    return null;
  }

  /**
   * Entity names keyed by lower case, so GraphQL's MLModel and the registry's mlModel line up
   * without a hand-maintained translation table.
   *
   * <p>Built from {@link EntitySpec#getName()}, not from the key set: {@link ConfigEntityRegistry}
   * lower cases its map keys but leaves the spec's own name in the casing entity-registry.yml
   * declares. Keying off the map would make {@code datajob} the canonical spelling and never match
   * the {@code dataJob} that the PDL and the yml use.
   */
  private static Map<String, String> indexEntityNames(final EntityRegistry registry) {
    final Map<String, String> index = new TreeMap<>();
    registry
        .getEntitySpecs()
        .values()
        .forEach(spec -> index.put(spec.getName().toLowerCase(Locale.ROOT), spec.getName()));
    return index;
  }
}
