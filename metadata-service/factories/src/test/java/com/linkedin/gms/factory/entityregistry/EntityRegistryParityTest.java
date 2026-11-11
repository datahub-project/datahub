package com.linkedin.gms.factory.entityregistry;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.timeline.eventgenerator.EntityChangeEventGeneratorRegistryFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.search.EntityTypeListConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.timeline.eventgenerator.EntityKeyChangeEventGenerator;
import io.datahubproject.metadata.context.OperationContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Guards against registering an entity type or aspect in {@code entity-registry.yml} without also
 * wiring it into the places that sibling entities are wired into: authorization resource types,
 * timeline change-event generators, the default search / autocomplete / browse lists, and patch
 * templates.
 *
 * <p>"Siblings" are the catalog entities in the default search list ({@code
 * elasticsearch.search.defaultEntityTypes} in application.yaml): the types users search for and
 * expect to browse, autocomplete, write policies against and receive change events for.
 *
 * <p>Each {@code *_BASELINE} set lists known gaps. A baseline may only shrink: the test fails when
 * a new gap appears, and also when a baselined gap has been fixed but is still listed. Fix the gap
 * rather than adding to a baseline; if an exception is intentional, add it with a comment saying
 * why.
 */
public class EntityRegistryParityTest {

  private static final String SEARCH_PREFIX = "elasticsearch.search.";

  // ---------------------------------------------------------------------------------------------
  // Search / autocomplete / browse
  // ---------------------------------------------------------------------------------------------

  /** Searchable catalog entities that are not offered by autocomplete-across-entities. */
  private static final Set<String> AUTOCOMPLETE_BASELINE =
      Set.of(
          // Children of other entities; surfaced through their parents rather than on their own.
          "mlFeature",
          "mlPrimaryKey",
          "schemaField",
          // Known gaps.
          "glossaryNode",
          "role",
          "document");

  /** Searchable catalog entities with a browsePathsV2 aspect that are not in the browse list. */
  private static final Set<String> BROWSE_BASELINE =
      Set.of(
          // Browsed under its parent feature table.
          "mlFeature",
          // Known gaps: searchable and autocompleted, carry browsePathsV2, but not browsable.
          "metric",
          "semanticModel");

  // ---------------------------------------------------------------------------------------------
  // Authorization
  // ---------------------------------------------------------------------------------------------

  /** Searchable catalog entities with no resource type in PoliciesConfig. */
  private static final Set<String> POLICY_RESOURCE_BASELINE =
      Set.of(
          // Authorized through the parent dataset.
          "schemaField",
          // Known gaps: policies can only target these through the "all" resource type.
          "api",
          "metric",
          "mlModelGroup",
          "mlPrimaryKey",
          "role",
          "semanticModel");

  /**
   * Entity-scoped privileges that edit one specific aspect. If a policy resource type's entity
   * carries the aspect, the resource type must offer the privilege; otherwise a policy cannot grant
   * editing that aspect for just that type. (The reverse is not checked: many resource types share
   * COMMON_ENTITY_PRIVILEGES and so offer privileges for aspects they do not have.)
   */
  private static final Map<String, String> PRIVILEGE_TO_ASPECT =
      Map.of(
          PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType(), Constants.OWNERSHIP_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType(), Constants.GLOBAL_TAGS_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType(),
              Constants.GLOSSARY_TERMS_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_DOMAINS_PRIVILEGE.getType(), Constants.DOMAINS_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType(),
              Constants.DEPRECATION_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_DOC_LINKS_PRIVILEGE.getType(),
              Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType(),
              Constants.STRUCTURED_PROPERTIES_ASPECT_NAME,
          PoliciesConfig.EDIT_ENTITY_APPLICATIONS_PRIVILEGE.getType(),
              Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);

  /** "resourceType:privilege" pairs where the aspect exists but the privilege is not offered. */
  private static final Set<String> ASPECT_WITHOUT_PRIVILEGE_BASELINE =
      Set.of(
          // Known gaps: not offered today, so only an "all"-type policy can grant them. If one
          // turns
          // out to be intentional, list it in its own group with a comment saying why.
          "businessAttribute:EDIT_ENTITY_DOC_LINKS",
          "corpGroup:EDIT_ENTITY_TAGS",
          "corpuser:EDIT_ENTITY_TAGS",
          "dataHubIngestionSource:EDIT_ENTITY_OWNERS",
          "dataPlatformInstance:EDIT_DEPRECATION_PRIVILEGE",
          "dataPlatformInstance:EDIT_ENTITY_DOC_LINKS",
          "dataPlatformInstance:EDIT_ENTITY_OWNERS",
          "dataPlatformInstance:EDIT_ENTITY_TAGS",
          "dataProduct:EDIT_DEPRECATION_PRIVILEGE",
          "dataProduct:EDIT_ENTITY_APPLICATIONS_PRIVILEGE",
          "domain:EDIT_DEPRECATION_PRIVILEGE",
          "glossaryNode:EDIT_DOMAINS_PRIVILEGE",
          "glossaryNode:EDIT_ENTITY_APPLICATIONS_PRIVILEGE",
          "glossaryTerm:EDIT_DOMAINS_PRIVILEGE",
          "glossaryTerm:EDIT_ENTITY_APPLICATIONS_PRIVILEGE",
          "structuredProperty:EDIT_ENTITY_DOC_LINKS",
          "tag:EDIT_DEPRECATION_PRIVILEGE");

  // ---------------------------------------------------------------------------------------------
  // Timeline change events
  // ---------------------------------------------------------------------------------------------

  /**
   * Searchable catalog entities whose key aspect has no EntityKeyChangeEventGenerator in
   * EntityChangeEventGeneratorRegistryFactory.
   *
   * <p>This covers the factory side only. A lifecycle event is emitted only when the key aspect is
   * also in PlatformEventGeneratorHook's supported aspects (mae-consumer), which this module cannot
   * see. Known hook-side gap, tracked separately: corpGroup passes here, but corpGroupKey is
   * missing from the hook, so corpGroup emits no lifecycle event either.
   */
  private static final Set<String> KEY_CHANGE_EVENT_BASELINE =
      Set.of(
          // Lifecycle follows the parent dataset's schemaMetadata.
          "schemaField",
          // Known gaps.
          "api",
          "application",
          "corpuser",
          "dataProduct",
          "document",
          "glossaryNode",
          "metric",
          "mlFeature",
          "mlFeatureTable",
          "mlModel",
          "mlModelGroup",
          "mlPrimaryKey",
          "notebook",
          "role",
          "semanticModel");

  // ---------------------------------------------------------------------------------------------
  // Patch
  // ---------------------------------------------------------------------------------------------

  /**
   * Aspects that have a Java patch builder but no patch template in the template engine. A
   * registered template whose getDefault() throws is a separate failure with no baseline: PATCH on
   * an entity that lacks the aspect needs that default.
   */
  private static final Set<String> PATCH_TEMPLATE_BASELINE =
      Set.of(
          // IncidentInfoPatchBuilder always sends forceGenericPatch, which PatchItemImpl applies
          // with GenericPatchTemplate, so it needs no registered template.
          Constants.INCIDENT_INFO_ASPECT_NAME,
          // Known gap: PATCH fails with UnsupportedOperationException when the aspect does not
          // exist yet and with an NPE when it does. AssertionRunSummaryHook (mae-consumer) sends
          // these patches.
          Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME);

  /** Generator registrations whose key is not an aspect name in the registry. */
  private static final Set<String> GENERATOR_ASPECT_ALLOWLIST = Set.of();

  private EntityRegistry registry;
  private Set<String> searchTypes;

  @BeforeClass
  public void setUp() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    registry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    searchTypes = yamlEntityTypes("defaultEntityTypes");
  }

  // ---------------------------------------------------------------------------------------------
  // Search / autocomplete / browse
  // ---------------------------------------------------------------------------------------------

  @Test
  public void entityTypeListsMatchMirroredConstantsAndRegistry() {
    Map<String, String> mirrored =
        Map.of(
            "defaultEntityTypes", EntityTypeListConfig.DEFAULT_SEARCH_ENTITY_TYPES,
            "autocompleteEntityTypes", EntityTypeListConfig.DEFAULT_AUTOCOMPLETE_ENTITY_TYPES,
            "browseEntityTypes", EntityTypeListConfig.DEFAULT_BROWSE_ENTITY_TYPES,
            "prioritizedSourceEntityTypes",
                EntityTypeListConfig.DEFAULT_PRIORITIZED_SOURCE_ENTITY_TYPES,
            "prioritizedDatahubEntityTypes",
                EntityTypeListConfig.DEFAULT_PRIORITIZED_DATAHUB_ENTITY_TYPES);
    Set<String> unknown = new TreeSet<>();
    for (Map.Entry<String, String> entry : mirrored.entrySet()) {
      Set<String> fromYaml = yamlEntityTypes(entry.getKey());
      // Compared as lists: the prioritized* lists are order-sensitive.
      assertEquals(
          List.copyOf(fromYaml),
          List.copyOf(csv(entry.getValue())),
          entry.getKey()
              + " in application.yaml and its EntityTypeListConfig constant have drifted apart");
      fromYaml.stream()
          .filter(name -> !isEntity(name))
          .forEach(name -> unknown.add(entry.getKey() + ":" + name));
    }
    assertTrue(unknown.isEmpty(), "Entity type lists name unregistered entities: " + unknown);
  }

  @Test
  public void searchableEntitiesAreInAutocomplete() {
    Set<String> autocomplete = yamlEntityTypes("autocompleteEntityTypes");
    assertBaseline(
        "Searchable entities missing from autocompleteEntityTypes",
        missing(searchTypes, autocomplete),
        AUTOCOMPLETE_BASELINE);
  }

  @Test
  public void browsableSearchableEntitiesAreInBrowse() {
    Set<String> browse = yamlEntityTypes("browseEntityTypes");
    Set<String> browsable =
        searchTypes.stream()
            .filter(name -> hasAspect(name, Constants.BROWSE_PATHS_V2_ASPECT_NAME))
            .collect(Collectors.toCollection(TreeSet::new));
    assertBaseline(
        "Searchable entities with browsePathsV2 missing from browseEntityTypes",
        missing(browsable, browse),
        BROWSE_BASELINE);
  }

  // ---------------------------------------------------------------------------------------------
  // Authorization
  // ---------------------------------------------------------------------------------------------

  @Test
  public void searchableEntitiesHavePolicyResourceType() {
    Set<String> resourceTypes = policyResourceTypes().keySet();
    Set<String> unknown =
        resourceTypes.stream()
            .filter(name -> !isEntity(name))
            .collect(Collectors.toCollection(TreeSet::new));
    assertTrue(unknown.isEmpty(), "PoliciesConfig names unregistered entities: " + unknown);
    assertBaseline(
        "Searchable entities without a ResourcePrivileges entry in PoliciesConfig",
        missing(searchTypes, resourceTypes),
        POLICY_RESOURCE_BASELINE);
  }

  @Test
  public void entityAspectsHaveEditPrivileges() {
    Set<String> aspectWithoutPrivilege = new TreeSet<>();
    policyResourceTypes()
        .forEach(
            (resourceType, privileges) -> {
              for (Map.Entry<String, String> entry : PRIVILEGE_TO_ASPECT.entrySet()) {
                if (hasAspect(resourceType, entry.getValue())
                    && !privileges.contains(entry.getKey())) {
                  aspectWithoutPrivilege.add(resourceType + ":" + entry.getKey());
                }
              }
            });
    assertBaseline(
        "Entities with an aspect but without the privilege that edits it",
        aspectWithoutPrivilege,
        ASPECT_WITHOUT_PRIVILEGE_BASELINE);
  }

  // ---------------------------------------------------------------------------------------------
  // Timeline change events
  // ---------------------------------------------------------------------------------------------

  @Test
  public void changeEventGeneratorsCoverRegisteredAspects() {
    EntityChangeEventGeneratorRegistry generators = changeEventGeneratorRegistry();

    Set<String> allAspects =
        registry.getEntitySpecs().values().stream()
            .flatMap(spec -> spec.getAspectSpecMap().keySet().stream())
            .collect(Collectors.toSet());
    Set<String> unknownAspects =
        generatorAspects(generators).stream()
            .filter(aspect -> !allAspects.contains(aspect))
            .filter(aspect -> !GENERATOR_ASPECT_ALLOWLIST.contains(aspect))
            .collect(Collectors.toCollection(TreeSet::new));
    assertTrue(
        unknownAspects.isEmpty(),
        "Change event generators registered for unknown aspects: " + unknownAspects);

    Set<String> missingKeyGenerators = new TreeSet<>();
    for (String entity : searchTypes) {
      String keyAspect = spec(entity).getKeyAspectName();
      boolean hasKeyGenerator =
          generators.getEntityChangeEventGenerators(keyAspect).stream()
              .anyMatch(EntityKeyChangeEventGenerator.class::isInstance);
      if (!hasKeyGenerator) {
        missingKeyGenerators.add(entity);
      }
    }
    assertBaseline(
        "Searchable entities whose key aspect has no EntityKeyChangeEventGenerator",
        missingKeyGenerators,
        KEY_CHANGE_EVENT_BASELINE);
  }

  // ---------------------------------------------------------------------------------------------
  // Patch
  // ---------------------------------------------------------------------------------------------

  @Test
  public void patchBuildersHaveTemplates() throws Exception {
    AspectTemplateEngine templates = SnapshotEntityRegistry.getInstance().getAspectTemplateEngine();
    Map<String, Template<?>> registeredTemplates = templateMap(templates);
    Method aspectName = AbstractMultiFieldPatchBuilder.class.getDeclaredMethod("getAspectName");
    Method entityType = AbstractMultiFieldPatchBuilder.class.getDeclaredMethod("getEntityType");
    // Reflection: builders expose their aspect/entity only via protected methods, and a builder
    // cannot build() without a urn and at least one patch operation.
    aspectName.setAccessible(true);
    entityType.setAccessible(true);

    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false);
    scanner.addIncludeFilter(new AssignableTypeFilter(AbstractMultiFieldPatchBuilder.class));
    List<Class<?>> builders =
        scanner
            .findCandidateComponents(AbstractMultiFieldPatchBuilder.class.getPackageName())
            .stream()
            .map(bd -> loadClass(bd.getBeanClassName()))
            .collect(Collectors.toList());
    assertTrue(builders.size() > 10, "Classpath scan found too few patch builders: " + builders);

    Set<String> unknown = new TreeSet<>();
    Set<String> withoutTemplate = new TreeSet<>();
    Set<String> defaultThrows = new TreeSet<>();
    for (Class<?> builderClass : builders) {
      Object builder = builderClass.getDeclaredConstructor().newInstance();
      String aspect = (String) aspectName.invoke(builder);
      String entity = fixedEntityType(entityType, builder);
      if (entity != null && !hasAspect(entity, aspect)) {
        unknown.add(builderClass.getSimpleName() + " -> " + entity + "." + aspect);
      }
      Template<?> template = registeredTemplates.get(aspect);
      if (template == null) {
        withoutTemplate.add(aspect);
      } else {
        try {
          template.getDefault();
        } catch (Exception e) {
          defaultThrows.add(aspect + " (" + e + ")");
        }
      }
    }
    assertTrue(unknown.isEmpty(), "Patch builders target unregistered aspects: " + unknown);
    assertTrue(
        defaultThrows.isEmpty(),
        "Patch templates whose getDefault() throws, so PATCH fails when the aspect does not exist"
            + " yet: "
            + defaultThrows);
    assertBaseline(
        "Aspects with a patch builder but no patch template in SnapshotEntityRegistry",
        withoutTemplate,
        PATCH_TEMPLATE_BASELINE);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static void assertBaseline(String what, Set<String> actual, Set<String> baseline) {
    Set<String> newGaps = new TreeSet<>(actual);
    newGaps.removeAll(baseline);
    Set<String> fixed = new TreeSet<>(baseline);
    fixed.removeAll(actual);
    assertTrue(
        newGaps.isEmpty(),
        what + ": " + newGaps + ". Register them, or baseline them with a reason.");
    assertTrue(fixed.isEmpty(), what + ": " + fixed + " are fixed; remove them from the baseline.");
  }

  private static Set<String> missing(Set<String> expected, Set<String> actual) {
    return expected.stream()
        .filter(name -> !actual.contains(name))
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private boolean isEntity(String name) {
    return registry.getEntitySpecs().containsKey(name.toLowerCase());
  }

  private EntitySpec spec(String entity) {
    return registry.getEntitySpec(entity);
  }

  private boolean hasAspect(String entity, String aspect) {
    return isEntity(entity) && spec(entity).hasAspect(aspect);
  }

  private static Map<String, Set<String>> policyResourceTypes() {
    return PoliciesConfig.ENTITY_RESOURCE_PRIVILEGES.stream()
        .collect(
            Collectors.toMap(
                PoliciesConfig.ResourcePrivileges::getResourceType,
                rp ->
                    rp.getPrivileges().stream()
                        .map(PoliciesConfig.Privilege::getType)
                        .collect(Collectors.toSet())));
  }

  private static EntityChangeEventGeneratorRegistry changeEventGeneratorRegistry() {
    ExposedGeneratorFactory factory = new ExposedGeneratorFactory();
    // Reflection: the factory reads this @Autowired field before building; the value is unused.
    ReflectionTestUtils.setField(factory, "applicationContext", mock(ApplicationContext.class));
    return factory.build(mock(OperationContext.class), mock(SystemEntityClient.class));
  }

  @SuppressWarnings("unchecked")
  private static Set<String> generatorAspects(EntityChangeEventGeneratorRegistry generators) {
    // Reflection: the registry only offers lookup by aspect name, not the set of registered names.
    Map<String, Set<EntityChangeEventGenerator<?>>> byAspect =
        (Map<String, Set<EntityChangeEventGenerator<?>>>)
            ReflectionTestUtils.getField(generators, "entityChangeEventGenerators");
    return byAspect.keySet();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Template<?>> templateMap(AspectTemplateEngine engine) {
    // Reflection: getDefaultTemplate() returns null both for a missing template and for one whose
    // getDefault() throws, so the registered map is the only way to tell them apart.
    return (Map<String, Template<?>>) ReflectionTestUtils.getField(engine, "_aspectTemplateMap");
  }

  /**
   * Reads the built-in default of a {@code ${ENV:default}} entity type list in application.yaml.
   */
  private static Set<String> yamlEntityTypes(String listName) {
    YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
    yaml.setResources(new ClassPathResource("application.yaml"));
    Properties properties = yaml.getObject();
    String raw = properties.getProperty(SEARCH_PREFIX + listName + ".value");
    Matcher matcher = Pattern.compile("^\\$\\{[A-Z0-9_]+:(.*)}$").matcher(raw.trim());
    assertTrue(matcher.matches(), "Unexpected format for " + listName + ": " + raw);
    return csv(matcher.group(1));
  }

  private static Set<String> csv(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Returns the builder's entity type, or null for builders of aspects shared by many entities
   * (ownership, globalTags, ...), which take it from the target urn and throw until one is set.
   */
  private static String fixedEntityType(Method entityType, Object builder) throws Exception {
    try {
      return (String) entityType.invoke(builder);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof IllegalStateException) {
        return null;
      }
      throw e;
    }
  }

  private static Class<?> loadClass(String name) {
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Exposes the protected bean method so the real generator wiring is tested, not a copy. */
  private static class ExposedGeneratorFactory extends EntityChangeEventGeneratorRegistryFactory {
    EntityChangeEventGeneratorRegistry build(
        OperationContext opContext, SystemEntityClient client) {
      return entityChangeEventGeneratorRegistry(opContext, client);
    }
  }
}
