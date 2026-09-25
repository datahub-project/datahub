package com.linkedin.datahub.graphql;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * Guards against the {@code Role.isAssignedToMe} bug class: a resolver wired to a field on an
 * Entity type that reads a SIBLING field off {@code environment.getSource()}. The sibling is only
 * populated when its backing aspect is fetched, and under aspect optimization that aspect is only
 * fetched if the wired field's {@code @aspectMapping} names it. {@code IsAssignedToMeResolver} read
 * {@code role.getActors()} while {@code isAssignedToMe} was {@code @noAspects}, so assigned users
 * silently saw {@code false}.
 *
 * <p>Two source-read shapes are safe and expected:
 *
 * <ul>
 *   <li><b>Self-consistent</b> — the resolver reads the same field it is wired to (e.g. {@code
 *       platform} reading {@code getPlatform()}): the field's own annotation hydrates the stub.
 *   <li><b>Nested-type parents</b> — the source is a non-entity type (e.g. {@code ActorFilter},
 *       {@code Status}, {@code FormActorAssignment}): reaching it required selecting the annotated
 *       entity-level field that populated it.
 * </ul>
 *
 * <p>Anything else must be added to an allowlist below with a justification, after verifying the
 * wired field's annotation covers every aspect that populates the getters it reads.
 */
public class ResolverSourceReadGuardTest {

  /** Resolver classes that read typed source getters, each reviewed and justified. */
  private static final Map<String, String> ALLOWED_RESOLVER_CLASSES =
      Map.of(
          "IsAssignedToMeResolver",
          "reads Role.getActors(); Role.isAssignedToMe maps [\"actors\"] (entity.graphql)",
          "IsFormAssignedToMeResolver",
          "source is nested FormActorAssignment, reachable only via Form.info -> formInfo",
          "StatusLifecycleStageResolver",
          "source is nested Status, populated via the entity's status field annotation");

  /**
   * Wiring-lambda reads where the getter set differs from the wired field, keyed as "field[getterA,
   * getterB]", each reviewed and justified.
   */
  private static final Map<String, String> ALLOWED_WIRING_READS =
      Map.of(
          "resolvedUsers[Users]",
          "ActorFilter is nested under Policy.actors -> dataHubPolicyInfo",
          "resolvedGroups[Groups]",
          "ActorFilter is nested under Policy.actors -> dataHubPolicyInfo",
          "resolvedRoles[Roles]",
          "ActorFilter is nested under Policy.actors -> dataHubPolicyInfo",
          "resolvedOwnershipTypes[ResourceOwnersTypes]",
          "ActorFilter is nested under Policy.actors -> dataHubPolicyInfo",
          "source[IngestionSource, Input]",
          "ExecutionRequest.source maps [\"dataHubExecutionRequestInput\"], which populates input",
          "actor[ActorUrn]",
          "ResolvedAuditStamp is a nested type populated by its parent's annotated field");

  private static final Pattern CHAINED_SOURCE_GETTER =
      Pattern.compile("getSource\\(\\)\\)?\\s*\\.get([A-Za-z0-9]+)\\(");
  private static final Pattern SOURCE_VARIABLE =
      Pattern.compile(
          "[A-Za-z0-9_<>.\\[\\]]+\\s+([a-zA-Z0-9_]+)\\s*=\\s*(?:\\([^)]*\\)\\s*)?"
              + "env(?:ironment)?\\.getSource\\(\\);");
  private static final Pattern DATA_FETCHER =
      Pattern.compile("\\.dataFetcher\\(\\s*\"([A-Za-z0-9]+)\"\\s*,");

  private static final Set<String> IDENTITY_GETTERS = Set.of("urn", "type", "class");

  private static Path sourceRoot() {
    for (Path candidate :
        List.of(
            Paths.get("src/main/java/com/linkedin/datahub/graphql"),
            Paths.get("datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql"))) {
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
    }
    throw new IllegalStateException(
        "Could not locate datahub-graphql-core sources from " + Paths.get("").toAbsolutePath());
  }

  private static Set<String> sourceGetters(String text) {
    Set<String> getters = new HashSet<>();
    Matcher chained = CHAINED_SOURCE_GETTER.matcher(text);
    while (chained.find()) {
      getters.add(chained.group(1));
    }
    Matcher variable = SOURCE_VARIABLE.matcher(text);
    while (variable.find()) {
      Matcher use =
          Pattern.compile(Pattern.quote(variable.group(1)) + "\\.get([A-Za-z0-9]+)\\(")
              .matcher(text);
      while (use.find()) {
        getters.add(use.group(1));
      }
    }
    getters.removeIf(g -> IDENTITY_GETTERS.contains(g.toLowerCase()));
    return getters;
  }

  /** Balanced-paren span of the dataFetcher argument list starting at {@code from}. */
  private static String argumentSpan(String text, int from) {
    int depth = 1;
    int i = from;
    while (i < text.length() && depth > 0) {
      char c = text.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      }
      i++;
    }
    return text.substring(from, Math.max(from, i - 1));
  }

  private static List<Path> mainSources() throws IOException {
    try (Stream<Path> walk = Files.walk(sourceRoot())) {
      return walk.filter(p -> p.toString().endsWith(".java")).collect(Collectors.toList());
    }
  }

  @Test
  public void testResolverClassesReadingTypedSourceGettersAreAllowlisted() throws IOException {
    List<String> violations = new ArrayList<>();
    for (Path file : mainSources()) {
      String name = file.getFileName().toString().replace(".java", "");
      String text = Files.readString(file);
      // Wiring files are covered field-by-field by the other test; mappers read their own DTOs.
      if (text.contains(".dataFetcher(") || name.endsWith("Mapper")) {
        continue;
      }
      if (!text.contains("getSource()")) {
        continue;
      }
      Set<String> getters = sourceGetters(text);
      if (!getters.isEmpty() && !ALLOWED_RESOLVER_CLASSES.containsKey(name)) {
        violations.add(name + " reads source getters " + new TreeSet<>(getters));
      }
    }
    assertTrue(
        violations.isEmpty(),
        "Resolver classes read typed getSource() getters without being reviewed: "
            + violations
            + ". A sibling field on an Entity source is only populated when its backing aspect is"
            + " fetched, which under aspect optimization requires the WIRED field's @aspectMapping"
            + " to name that aspect (see Role.isAssignedToMe). Either annotate the wired field with"
            + " the aspects populating these getters, or add the class to ALLOWED_RESOLVER_CLASSES"
            + " with a justification.");
  }

  @Test
  public void testWiringLambdasOnlyReadTheWiredFieldOrAllowlistedGetters() throws IOException {
    List<String> violations = new ArrayList<>();
    for (Path file : mainSources()) {
      String text = Files.readString(file);
      if (!text.contains(".dataFetcher(")) {
        continue;
      }
      Matcher wiring = DATA_FETCHER.matcher(text);
      while (wiring.find()) {
        String field = wiring.group(1);
        Set<String> getters = sourceGetters(argumentSpan(text, wiring.end()));
        getters.removeIf(g -> g.equalsIgnoreCase(field));
        if (getters.isEmpty()) {
          continue;
        }
        String key = field + new TreeSet<>(getters);
        if (!ALLOWED_WIRING_READS.containsKey(key)) {
          violations.add(file.getFileName() + ": " + key);
        }
      }
    }
    if (!violations.isEmpty()) {
      fail(
          "dataFetcher lambdas read source getters other than the wired field: "
              + violations
              + ". If the source is the Entity itself, the wired field's @aspectMapping must"
              + " include the aspects populating those getters (see Role.isAssignedToMe); if the"
              + " source is a nested type populated via an annotated entity-level field, add the"
              + " \"field[getters]\" key to ALLOWED_WIRING_READS with a justification.");
    }
  }
}
