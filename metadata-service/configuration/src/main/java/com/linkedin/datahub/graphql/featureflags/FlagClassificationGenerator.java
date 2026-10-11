package com.linkedin.datahub.graphql.featureflags;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generates scripts/generated/flag-classification.json from two sources of truth:
 *
 * <ul>
 *   <li><b>Dynamic flags</b> — reflected from {@link FeatureFlags} boolean fields. Inspectable via
 *       the /openapi/operations/dev/featureFlags API. All changes require a container restart via
 *       {@code env set} + {@code env restart}.
 *   <li><b>Static flags</b> — env vars found in application.yaml outside the featureFlags section.
 *       These require a container restart to take effect.
 * </ul>
 *
 * <p>Run via Gradle: {@code ./gradlew :metadata-service:configuration:generateFlagClassification}
 */
public class FlagClassificationGenerator {

  private static final Pattern ENV_VAR_PATTERN =
      Pattern.compile("\\$\\{([A-Z][A-Z0-9_]*)(?::[^}]*)?\\}");

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: FlagClassificationGenerator <output-file>");
      System.exit(1);
    }

    Path outputPath = Paths.get(args[0]);
    Path parent = outputPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    Map<String, Object> dynamicFlags = extractDynamicFlags();
    Map<String, Object> staticFlags = extractStaticFlags(dynamicFlags.keySet());

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    ObjectNode root = mapper.createObjectNode();

    root.put("_generated_at", Instant.now().toString());
    root.put(
        "_description",
        "Auto-generated from FeatureFlags.java and application.yaml."
            + " Regenerate with: ./gradlew :metadata-service:configuration:generateFlagClassification");

    ObjectNode dynamicNode = root.putObject("dynamic");
    for (Map.Entry<String, Object> entry : dynamicFlags.entrySet()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> info = (Map<String, Object>) entry.getValue();
      ObjectNode flagNode = dynamicNode.putObject(entry.getKey());
      flagNode.put("field", (String) info.get("field"));
      flagNode.put("default", (Boolean) info.get("default"));
    }

    ObjectNode staticNode = root.putObject("static");
    for (Map.Entry<String, Object> entry : staticFlags.entrySet()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> info = (Map<String, Object>) entry.getValue();
      ObjectNode flagNode = staticNode.putObject(entry.getKey());
      flagNode.put("reason", (String) info.get("reason"));
      flagNode.put("restart_required", (Boolean) info.get("restart_required"));
    }

    mapper.writeValue(outputPath.toFile(), root);
    System.out.println(
        "Generated: "
            + outputPath
            + " ("
            + dynamicFlags.size()
            + " dynamic, "
            + staticFlags.size()
            + " static flags)");
  }

  private static Map<String, Object> extractDynamicFlags() throws Exception {
    Map<String, Object> flags = new LinkedHashMap<>();
    FeatureFlags defaults = new FeatureFlags();

    for (Field f : FeatureFlags.class.getDeclaredFields()) {
      if (f.getType() != boolean.class) {
        continue;
      }
      f.setAccessible(true);
      boolean defaultVal = (boolean) f.get(defaults);
      String envName = camelToUpperUnderscore(f.getName());

      Map<String, Object> info = new LinkedHashMap<>();
      info.put("field", f.getName());
      info.put("default", defaultVal);
      flags.put(envName, info);
    }

    return flags;
  }

  /**
   * Scans application.yaml for ${ENV_VAR:default} patterns, skipping the featureFlags section
   * (already covered as dynamic flags) and any env vars already captured as dynamic.
   */
  private static Map<String, Object> extractStaticFlags(Set<String> excludeEnvVars)
      throws Exception {
    Map<String, Object> flags = new LinkedHashMap<>();

    try (InputStream is =
        FlagClassificationGenerator.class.getResourceAsStream("/application.yaml")) {
      if (is == null) {
        System.err.println("WARNING: application.yaml not found on classpath");
        return flags;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        String line;
        boolean inFeatureFlagsSection = false;
        int featureFlagsIndent = -1;

        while ((line = reader.readLine()) != null) {
          String trimmed = line.stripLeading();
          int currentIndent = line.length() - trimmed.length();

          if (trimmed.startsWith("featureFlags:")) {
            inFeatureFlagsSection = true;
            featureFlagsIndent = currentIndent;
            continue;
          }

          if (inFeatureFlagsSection) {
            // Exit featureFlags section when we see a non-empty, non-comment line at the same or
            // lower indentation level as the featureFlags: key itself.
            if (!trimmed.isEmpty()
                && !trimmed.startsWith("#")
                && currentIndent <= featureFlagsIndent) {
              inFeatureFlagsSection = false;
            } else {
              continue;
            }
          }

          Matcher m = ENV_VAR_PATTERN.matcher(line);
          while (m.find()) {
            String envVar = m.group(1);
            if (!excludeEnvVars.contains(envVar)) {
              Map<String, Object> info = new LinkedHashMap<>();
              info.put("reason", "startup configuration (@Value / infrastructure)");
              info.put("restart_required", true);
              flags.putIfAbsent(envVar, info);
            }
          }
        }
      }
    }

    return flags;
  }

  /**
   * Converts a camelCase field name to UPPER_UNDERSCORE env var format.
   *
   * <p>Handles acronyms correctly: {@code alternateMCPValidation} → {@code
   * ALTERNATE_MCP_VALIDATION}, {@code showBrowseV2} → {@code SHOW_BROWSE_V2}.
   */
  static String camelToUpperUnderscore(String name) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isUpperCase(c)) {
        // Insert underscore when transitioning from lowercase to uppercase
        if (i > 0 && Character.isLowerCase(name.charAt(i - 1))) {
          result.append('_');
          // Insert underscore at the end of an acronym run (e.g., "MCP" before "Validation")
        } else if (i > 0
            && Character.isUpperCase(name.charAt(i - 1))
            && i + 1 < name.length()
            && Character.isLowerCase(name.charAt(i + 1))) {
          result.append('_');
        }
        result.append(c);
      } else {
        result.append(Character.toUpperCase(c));
      }
    }
    return result.toString();
  }
}
