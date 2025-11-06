package io.datahubproject.metadata.services;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Thread-safe secret masker for Java components.
 *
 * <p>Masks environment variable values in log messages by replacing them with ${VAR_NAME}
 * placeholders.
 *
 * <p><strong>Minimum Length Requirement</strong>: Only secrets with 3+ characters are masked, to
 * balance security with usability. Very short values like "a", "1", "on" are too common to mask
 * reliably.
 *
 * <p>Length Requirements:
 *
 * <ul>
 *   <li><strong>3+ characters</strong>: MASKED
 *   <li><strong>&lt; 3 characters</strong>: NOT MASKED (filtered out with debug log)
 * </ul>
 *
 * <p><strong>Performance</strong>: For large secret sets (50+), uses chunked patterns to avoid
 * regex compilation performance issues and potential ReDoS vulnerabilities.
 *
 * <p><strong>Thread Safety</strong>: This class is fully thread-safe. Pattern compilation happens
 * during construction (thread-confined). The mask() method creates thread-local Matcher instances
 * and uses ConcurrentHashMap for secret lookups.
 *
 * <p>Usage: SecretMasker masker = new SecretMasker(Set.of("DB_PASSWORD")); String masked =
 * masker.mask("password is secret123"); // Result: "password is ${DB_PASSWORD}"
 *
 * <p><strong>Warning</strong>: Secrets shorter than 3 characters will NOT be masked. Use secrets
 * with 3+ characters for proper masking protection.
 */
@Slf4j
public class SecretMasker {

  private final Map<String, String> secretValueToName;
  private final List<Pattern> secretPatterns;
  private final boolean enabled;

  // System vars to exclude (not secrets)
  private static final Set<String> EXCLUDED_VARS =
      Set.of(
          "PATH",
          "HOME",
          "USER",
          "LANG",
          "PWD",
          "SHELL",
          "TERM",
          "JAVA_HOME",
          "GRADLE_HOME",
          "CLASSPATH");

  // Minimum secret length to balance security with usability
  // Prevents false positives from very short strings like "a", "1", "on"
  private static final int MIN_SECRET_LENGTH = 3;

  // Maximum secrets per regex pattern to avoid compilation performance issues
  // Large alternations (|) in regex can cause exponential compilation time
  private static final int MAX_SECRETS_IN_SINGLE_PATTERN = 50;

  /**
   * Create masker for specified environment variables.
   *
   * @param envVarNames Names of environment variables to mask
   */
  public SecretMasker(@Nonnull Set<String> envVarNames) {
    this.secretValueToName = new ConcurrentHashMap<>();
    this.secretPatterns = new ArrayList<>();

    Set<String> varsToMask = new HashSet<>(envVarNames);
    varsToMask.removeAll(EXCLUDED_VARS);

    this.enabled = !varsToMask.isEmpty();

    if (!enabled) {
      log.debug("SecretMasker created with no variables - masking disabled");
      return;
    }

    // Collect values from environment
    int registered = 0;
    List<String> skippedShortSecrets = new ArrayList<>();

    for (String varName : varsToMask) {
      // Try environment variable first, fallback to system property (for testing)
      String value = System.getenv(varName);
      if (value == null) {
        value = System.getProperty(varName);
      }

      if (value != null && !value.isEmpty()) {
        if (value.length() >= MIN_SECRET_LENGTH) {
          secretValueToName.put(value, varName);
          registered++;
        } else {
          // Secret value is too short - will not be masked
          skippedShortSecrets.add(varName);
        }
      }
    }

    // Warn about skipped short secrets
    if (!skippedShortSecrets.isEmpty()) {
      log.warn(
          "Skipped {} secret(s) shorter than {} chars: {}. These values will NOT be masked. "
              + "Consider using secrets with {}+ characters (NIST minimum).",
          skippedShortSecrets.size(),
          MIN_SECRET_LENGTH,
          String.join(", ", skippedShortSecrets),
          MIN_SECRET_LENGTH);
    }

    if (registered == 0) {
      log.debug(
          "No secret values found for provided variable names (minimum length: {} chars)",
          MIN_SECRET_LENGTH);
      return;
    }

    // Build regex patterns (chunked for large sets)
    long startTime = System.currentTimeMillis();
    buildPatterns(secretValueToName.keySet());
    long duration = System.currentTimeMillis() - startTime;

    if (registered > MAX_SECRETS_IN_SINGLE_PATTERN) {
      log.debug(
          "Large secret set ({} secrets) - using {} chunked patterns. Compilation took {} ms",
          registered,
          secretPatterns.size(),
          duration);
    }

    log.info("SecretMasker initialized with {} secret value(s)", registered);
  }

  /**
   * Mask secrets in input string.
   *
   * <p>Thread safety: This method is thread-safe. Pattern.matcher() creates a new Matcher instance
   * per call, which is NOT shared between threads. We use StringBuilder (not StringBuffer) because
   * the Matcher and StringBuilder are both thread-local to this method invocation.
   *
   * <p>Security: Uses Matcher.quoteReplacement() to prevent regex injection attacks where secret
   * values containing regex metacharacters (like $, \) could be interpreted as replacement
   * patterns.
   *
   * @param input String potentially containing secrets
   * @return String with secrets replaced by ${VAR_NAME}
   */
  @Nonnull
  public String mask(@Nullable String input) {
    if (input == null || input.isEmpty() || !enabled || secretPatterns.isEmpty()) {
      return input != null ? input : "";
    }

    // Apply all patterns sequentially
    // For small secret sets (1 pattern), this is a single pass
    // For large secret sets (multiple patterns), we apply each pattern in order
    String result = input;
    for (Pattern pattern : secretPatterns) {
      // Pattern.matcher() creates a NEW Matcher instance per call (thread-local)
      // Pattern itself is thread-safe and immutable
      Matcher matcher = pattern.matcher(result);
      if (!matcher.find()) {
        continue; // Skip if this pattern doesn't match
      }

      // Secrets found - replace them
      matcher.reset();
      // Use StringBuilder (not StringBuffer) - no synchronization overhead needed
      // since this Matcher and StringBuilder are both local to this method call
      StringBuilder sb = new StringBuilder(result.length());

      while (matcher.find()) {
        String matched = matcher.group();
        String varName = secretValueToName.get(matched);
        if (varName != null) {
          // SECURITY: Matcher.quoteReplacement() prevents regex injection
          // Example: if secret is "foo$bar", without quoting, "$bar" would be
          // interpreted as a backreference instead of literal text
          matcher.appendReplacement(sb, Matcher.quoteReplacement("${" + varName + "}"));
        }
      }

      matcher.appendTail(sb);
      result = sb.toString();
    }

    return result;
  }

  /**
   * Extract ${VAR_NAME} references from recipe string.
   *
   * @param recipe Recipe content (YAML/JSON)
   * @return Set of environment variable names
   */
  @Nonnull
  public static Set<String> extractEnvVarReferences(@Nonnull String recipe) {
    Set<String> vars = new HashSet<>();

    // Pattern: ${VAR_NAME}
    Pattern pattern = Pattern.compile("\\$\\{([A-Z_][A-Z0-9_]*)\\}");
    Matcher matcher = pattern.matcher(recipe);

    while (matcher.find()) {
      vars.add(matcher.group(1));
    }

    return vars;
  }

  /**
   * Build regex patterns matching secret values.
   *
   * <p>For large secret sets, splits into multiple patterns to avoid regex compilation performance
   * issues and potential ReDoS vulnerabilities from massive alternations.
   *
   * <p>Ensures longest-match-first is preserved across all patterns by sorting before chunking.
   */
  private void buildPatterns(Set<String> values) {
    if (values.isEmpty()) {
      return;
    }

    // Sort by length (longest first) for proper matching across ALL patterns
    // This ensures a longer secret like "password123456" matches before "password123"
    List<String> sorted = new ArrayList<>(values);
    sorted.sort((a, b) -> Integer.compare(b.length(), a.length()));

    // Split into chunks to avoid regex compilation performance issues
    List<List<String>> chunks = new ArrayList<>();
    for (int i = 0; i < sorted.size(); i += MAX_SECRETS_IN_SINGLE_PATTERN) {
      int end = Math.min(i + MAX_SECRETS_IN_SINGLE_PATTERN, sorted.size());
      chunks.add(sorted.subList(i, end));
    }

    // Build pattern for each chunk
    for (List<String> chunk : chunks) {
      StringJoiner patternBuilder = new StringJoiner("|");
      for (String value : chunk) {
        // Escape special regex characters
        patternBuilder.add(Pattern.quote(value));
      }

      Pattern pattern = Pattern.compile(patternBuilder.toString());
      secretPatterns.add(pattern);
    }

    log.debug("Built {} regex pattern(s) for {} secret(s)", secretPatterns.size(), sorted.size());
  }

  /** Check if masker is enabled. */
  public boolean isEnabled() {
    return enabled && !secretPatterns.isEmpty();
  }
}
