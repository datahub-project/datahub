package com.linkedin.datahub.graphql.types.incident;

import static com.linkedin.datahub.graphql.Constants.INCIDENTS_SCHEMA_FILE;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
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
 *   <li>read API: {@code incident.graphql}, types with an {@code incidents} field. Decides whether
 *       anything can list them back.
 * </ol>
 *
 * <p>#18478 widened the PDL list to include mlModel, mlFeature, service and aiAgent, added
 * incidentsSummary for service and aiAgent only, and wired GraphQL for none of them. The result on
 * v1.7.0 is that raiseIncident accepts an mlModel URN, stores the incident against the model, and
 * nothing can read it back. The caller gets an incident URN and no reason to think anything went
 * wrong, which is worse than a clean rejection. See #18999 and #18911.
 *
 * <p>All three sets are derived from the sources themselves, so adding an entity to one layer and
 * forgetting the other two fails here rather than in production. This test is the guardrail only;
 * closing the gaps is #18911. Until they close, {@link #KNOWN_GAPS} carries them explicitly, and
 * that map is meant to shrink to empty.
 *
 * <p><b>Known limitation.</b> The read check inspects the SDL only, which is necessary but not
 * sufficient. Read support is itself two independently maintained things: the {@code extend type X
 * { incidents }} block in {@code incident.graphql}, and a hardcoded {@code entitiesWithIncidents}
 * list in {@code GmsGraphQLEngine}. A type present in the SDL but absent from that list resolves
 * through the default {@code PropertyDataFetcher} and returns null with no error, so this test
 * would pass on that half-finished state. The two agree today (Dataset, DataJob, DataFlow,
 * Dashboard, Chart), so nothing is currently misreported; the exposure is future drift. Deriving a
 * fourth set probably wants runtime resolver registration rather than scraping a Java literal.
 * Raised by @cnpierrepapi on #18999.
 */
public class IncidentEntitySupportConsistencyTest {

  private static final String INCIDENT_ENTITY = "incident";
  private static final String INCIDENT_INFO_ASPECT = "incidentInfo";
  private static final String INCIDENTS_SUMMARY_ASPECT = "incidentsSummary";
  private static final String INCIDENT_ON_RELATIONSHIP = "IncidentOn";
  private static final String INCIDENTS_FIELD = "incidents";
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

  /**
   * Types knowingly supported in one layer and not the others, as of this commit.
   *
   * <p>Every entry is a bug, not a design decision. Delete entries as #18911 lands. When this map
   * is empty, drop it and the assertion below becomes strict equality across all three sets.
   */
  private static final Map<String, String> KNOWN_GAPS =
      new TreeMap<>(
          Map.of(
              "mlModel", "#18911: writable since #18478, no incidentsSummary, no GraphQL read",
              "mlFeature", "#18911: writable since #18478, no incidentsSummary, no GraphQL read",
              "service", "#18478 added incidentsSummary but not the GraphQL read",
              "aiAgent", "#18478 added incidentsSummary but not the GraphQL read",
              "schemaField",
                  "writable in the PDL allowlist, no incidentsSummary, no GraphQL read"));

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

    // Names the test could not map back to an entity. Distinct from drift: it means the test has
    // gone stale against a convention change, and it must not be reported as a passing schema.
    final List<String> unmapped = new ArrayList<>();

    final Set<String> writable = writeAllowlist(registry, entityNamesByLowerCase, unmapped);
    final Set<String> summarised = entitiesDeclaringIncidentsSummary(registry);
    final Set<String> readable = graphQlReadableEntities(entityNamesByLowerCase, unmapped);

    assertTrue(
        unmapped.isEmpty(),
        "This test could not map every incident-bearing name back to an entity in "
            + REGISTRY_PATH
            + ". Teach it the new name rather than deleting the check.\n\n"
            + String.join("\n", unmapped)
            + "\n");

    // Guards against a source moving or a parse silently yielding nothing, which would make the
    // equality below vacuously true.
    assertTrue(
        writable.size() > 1,
        "derived an implausible write allowlist from " + PDL_PATH + ": " + writable);
    assertTrue(
        readable.size() > 1,
        "derived an implausible GraphQL read set from " + GRAPHQL_PATH + ": " + readable);

    final Set<String> mentionedAnywhere = new TreeSet<>(writable);
    mentionedAnywhere.addAll(summarised);
    mentionedAnywhere.addAll(readable);

    final List<String> problems =
        mentionedAnywhere.stream()
            .filter(entity -> !KNOWN_GAPS.containsKey(entity))
            .map(entity -> describeGap(entity, writable, summarised, readable))
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(ArrayList::new));

    // A gap that has closed must come off the allowlist, or the allowlist stops meaning anything.
    KNOWN_GAPS.forEach(
        (entity, reason) -> {
          if (describeGap(entity, writable, summarised, readable) != null) {
            return;
          }
          final String state =
              mentionedAnywhere.contains(entity)
                  ? "is consistent across all three layers now"
                  : "is no longer referenced by any of the three layers";
          problems.add(
              "  `"
                  + entity
                  + "` "
                  + state
                  + ". Remove it from KNOWN_GAPS, where it is currently excused as: "
                  + reason);
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
            + " (incidents field)\n\n"
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
   * @return null when the entity is consistent across all three layers
   */
  private static String describeGap(
      final String entity,
      final Set<String> writable,
      final Set<String> summarised,
      final Set<String> readable) {
    final boolean isWritable = writable.contains(entity);
    final boolean isSummarised = summarised.contains(entity);
    final boolean isReadable = readable.contains(entity);
    if (isWritable == isSummarised && isSummarised == isReadable) {
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
    } else {
      missing.add("an incidents field on its GraphQL type in " + GRAPHQL_PATH);
    }
    return "  `"
        + entity
        + "` "
        + String.join(" and ", present)
        + ", but is missing "
        + String.join(" and ", missing);
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
   * Entities whose GraphQL type carries an incidents field.
   *
   * <p>Loaded and parsed through the same {@link GmsGraphQLEngine#fileBasedSchema} call and the
   * same {@link SchemaParser} the engine itself uses, so this reads the schema the way production
   * does rather than pattern-matching the file. It stops short of {@code makeExecutableSchema}: an
   * assembled schema object needs every other SDL file plus the full runtime wiring (services and
   * resolvers), which is a heavier fixture than a schema-consistency check should require, and it
   * would not answer a different question.
   *
   * <p>{@code extend type X} is the shape incident.graphql uses to attach the field, so an
   * extension that cannot be mapped back to an entity is reported rather than skipped. Plain {@code
   * type X} declarations in this file are incident-domain types (Incident, EntityIncidentsResult),
   * so those count only when the name is an entity in its own right.
   */
  private static Set<String> graphQlReadableEntities(
      final Map<String, String> entityNamesByLowerCase, final List<String> unmapped) {
    final TypeDefinitionRegistry schema =
        new SchemaParser().parse(GmsGraphQLEngine.fileBasedSchema(INCIDENTS_SCHEMA_FILE));

    final Set<String> readable = new TreeSet<>();

    schema
        .objectTypeExtensions()
        .forEach(
            (typeName, extensions) -> {
              if (extensions.stream()
                  .noneMatch(IncidentEntitySupportConsistencyTest::hasIncidents)) {
                return;
              }
              final String entity =
                  resolveEntity(
                      typeName,
                      entityNamesByLowerCase,
                      unmapped,
                      "  GraphQL type `"
                          + typeName
                          + "` in "
                          + GRAPHQL_PATH
                          + " has an incidents field but does not map to an entity in "
                          + REGISTRY_PATH);
              if (entity != null) {
                readable.add(entity);
              }
            });

    schema.getTypes(ObjectTypeDefinition.class).stream()
        .filter(IncidentEntitySupportConsistencyTest::hasIncidents)
        .map(type -> entityNamesByLowerCase.get(type.getName().toLowerCase(Locale.ROOT)))
        .filter(Objects::nonNull)
        .forEach(readable::add);

    return readable;
  }

  private static boolean hasIncidents(final ObjectTypeDefinition type) {
    return type.getFieldDefinitions().stream()
        .map(FieldDefinition::getName)
        .anyMatch(INCIDENTS_FIELD::equals);
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
    final String direct = entityNamesByLowerCase.get(name.toLowerCase(Locale.ROOT));
    if (direct != null) {
      return direct;
    }
    if (name.endsWith(GRAPHQL_ENTITY_SUFFIX)) {
      final String stripped =
          name.substring(0, name.length() - GRAPHQL_ENTITY_SUFFIX.length())
              .toLowerCase(Locale.ROOT);
      final String suffixed = entityNamesByLowerCase.get(stripped);
      if (suffixed != null) {
        return suffixed;
      }
    }
    unmapped.add(failureMessage);
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
