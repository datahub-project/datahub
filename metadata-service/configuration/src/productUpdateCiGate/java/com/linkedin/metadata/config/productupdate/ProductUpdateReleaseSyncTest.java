package com.linkedin.metadata.config.productupdate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * CI gate keeping the bundled product update ("What's New" toast) JSONs in step with the releases
 * they advertise.
 *
 * <p>These files are both the air-gapped fallback served by {@code ProductUpdateService} and the
 * source the hosted product.datahub.com JSONs are published from, so a stale file means every
 * instance shows a toast for an old release. Nothing else in the build notices — {@code
 * ProductUpdateParser} happily serves whatever version the JSON names, and drops the toast silently
 * when a required field is missing.
 *
 * <p>If {@code i18n} is present, every non-English UI locale under {@code
 * datahub-web-react/src/i18n/locales} must have translated copy for the English fields the toast
 * actually uses.
 */
public class ProductUpdateReleaseSyncTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Mirrors ProductUpdateParser's required-field contract; the toast never renders if absent. */
  private static final List<String> REQUIRED_FIELDS = List.of("enabled", "id");

  /**
   * Copy fields the parser will overlay from {@code i18n.<locale>}. English lives at the top level;
   * {@code en} is not required under {@code i18n}.
   */
  private static final List<String> TRANSLATABLE_FIELDS =
      List.of("title", "header", "description", "primaryCtaText", "secondaryCtaText", "ctaText");

  private static final String DEFAULT_LOCALE = "en";

  private static final Path UI_LOCALES_DIR = Path.of("datahub-web-react/src/i18n/locales");

  private static final Pattern BUNDLED_IMAGE =
      Pattern.compile("https://raw\\.githubusercontent\\.com/datahub-project/datahub/[^/]+/(.+)");

  private Path repoRoot;

  @BeforeClass
  public void setUp() {
    repoRoot = resolveRepoRoot();
  }

  @DataProvider(name = "flavors")
  public Object[][] flavors() {
    return new Object[][] {{ProductUpdateFlavor.CORE}, {ProductUpdateFlavor.CLOUD}};
  }

  @Test(dataProvider = "flavors")
  public void testProductUpdateAdvertisesLatestRelease(@Nonnull ProductUpdateFlavor flavor)
      throws IOException {
    JsonNode json = readProductUpdate(flavor);

    for (String field : REQUIRED_FIELDS) {
      Assert.assertTrue(
          json.hasNonNull(field),
          flavor.jsonPath()
              + " is missing required field '"
              + field
              + "'. ProductUpdateParser drops the update entirely without it, so no toast renders.");
    }

    if (!json.get("enabled").asBoolean()) {
      // The toast is intentionally switched off for this flavor; it advertises nothing to check.
      return;
    }

    String declaredId = json.get("id").asText();
    ReleaseVersion declared =
        ReleaseVersion.parse(declaredId)
            .orElseThrow(
                () ->
                    new AssertionError(
                        flavor.jsonPath()
                            + " has id '"
                            + declaredId
                            + "', which is not a release version. Expected something like "
                            + flavor.latestRelease(repoRoot)
                            + "."));

    ReleaseVersion latest = flavor.latestRelease(repoRoot);
    Assert.assertTrue(
        latest.sameMinorSeries(declared), staleUpdateMessage(flavor, declared, latest));
  }

  @Test(dataProvider = "flavors")
  public void testProductUpdateIsInternallyConsistent(@Nonnull ProductUpdateFlavor flavor)
      throws IOException {
    JsonNode json = readProductUpdate(flavor);
    Optional<ReleaseVersion> declared = ReleaseVersion.parse(json.path("id").asText());
    if (declared.isEmpty()) {
      // testProductUpdateAdvertisesLatestRelease owns reporting an unusable id.
      return;
    }

    String ctaLink = effectiveCtaLink(json);
    if (!ctaLink.isBlank()) {
      Assert.assertTrue(
          declared.get().appearsIn(ctaLink),
          flavor.jsonPath()
              + " advertises "
              + declared.get()
              + " but its CTA link points elsewhere: "
              + ctaLink
              + ". Point it at this release's notes so the toast doesn't send users to an older"
              + " release.");
    }

    String description = json.path("description").asText("");
    Optional<ReleaseVersion> describedVersion = ReleaseVersion.highestIn(description);
    if (describedVersion.isPresent()) {
      Assert.assertTrue(
          declared.get().sameMinorSeries(describedVersion.get()),
          flavor.jsonPath()
              + " advertises "
              + declared.get()
              + " but its description names "
              + describedVersion.get()
              + ": \""
              + description
              + "\".");
    }
  }

  @Test(dataProvider = "flavors")
  public void testProductUpdateImageExists(@Nonnull ProductUpdateFlavor flavor) throws IOException {
    JsonNode json = readProductUpdate(flavor);
    String image = json.path("image").asText("");
    Matcher bundled = BUNDLED_IMAGE.matcher(image);
    if (!bundled.matches()) {
      // Externally hosted images can't be verified from the repo.
      return;
    }

    String imagePath = bundled.group(1);
    Assert.assertTrue(
        Files.isRegularFile(repoRoot.resolve(imagePath)),
        flavor.jsonPath()
            + " references image "
            + imagePath
            + ", which does not exist in the repo. The toast would render with a broken image.");
  }

  /**
   * Once a flavor JSON starts translating the toast, every UI locale other than English must have
   * an {@code i18n} object. English stays on the top-level fields. Absent or empty {@code i18n} is
   * skipped so air-gapped fallbacks can ship before copy is translated.
   */
  @Test(dataProvider = "flavors")
  public void testProductUpdateI18nCoversSupportedLocales(@Nonnull ProductUpdateFlavor flavor)
      throws IOException {
    JsonNode json = readProductUpdate(flavor);
    JsonNode i18n = json.get("i18n");
    if (i18n == null || i18n.isNull() || (i18n.isObject() && i18n.isEmpty())) {
      return;
    }

    Assert.assertTrue(
        i18n.isObject(), flavor.jsonPath() + " has i18n but it is not an object of locale keys.");

    Set<String> requiredLocales = supportedUiLocales(repoRoot);
    Set<String> presentLocales = new TreeSet<>();
    i18n.fieldNames().forEachRemaining(presentLocales::add);

    Set<String> missing = new TreeSet<>(requiredLocales);
    missing.removeAll(presentLocales);
    Set<String> unexpected = new TreeSet<>(presentLocales);
    unexpected.removeAll(requiredLocales);
    unexpected.remove(DEFAULT_LOCALE);

    Assert.assertTrue(
        missing.isEmpty() && unexpected.isEmpty(),
        flavor.jsonPath()
            + " i18n locales do not match UI languages in "
            + UI_LOCALES_DIR
            + ".\nMissing: "
            + missing
            + "\nUnexpected: "
            + unexpected
            + "\n"
            + DEFAULT_LOCALE
            + " uses the top-level English fields and should not be required under i18n.");

    Set<String> englishFields = presentTranslatableFields(json);
    for (String locale : requiredLocales) {
      JsonNode localeNode = i18n.get(locale);
      Assert.assertTrue(
          localeNode != null && localeNode.isObject(),
          flavor.jsonPath() + " i18n." + locale + " must be an object of translated strings.");
      assertLocaleHasEnglishCopy(flavor, locale, json, localeNode, englishFields);
    }
  }

  @Nonnull
  private static Set<String> supportedUiLocales(@Nonnull Path repoRoot) throws IOException {
    Path localesDir = repoRoot.resolve(UI_LOCALES_DIR);
    Assert.assertTrue(Files.isDirectory(localesDir), "Missing UI locale directory: " + localesDir);
    try (Stream<Path> children = Files.list(localesDir)) {
      Set<String> locales =
          children
              .filter(Files::isDirectory)
              .map(path -> path.getFileName().toString())
              .filter(name -> !name.startsWith("."))
              .filter(name -> !DEFAULT_LOCALE.equals(name))
              .collect(Collectors.toCollection(TreeSet::new));
      Assert.assertFalse(
          locales.isEmpty(), "Found no non-English locale directories under " + localesDir);
      return locales;
    }
  }

  @Nonnull
  private static Set<String> presentTranslatableFields(@Nonnull JsonNode json) {
    Set<String> fields = new TreeSet<>();
    for (String field : TRANSLATABLE_FIELDS) {
      if (hasCopy(json, field)) {
        fields.add(field);
      }
    }
    return fields;
  }

  private static void assertLocaleHasEnglishCopy(
      @Nonnull ProductUpdateFlavor flavor,
      @Nonnull String locale,
      @Nonnull JsonNode english,
      @Nonnull JsonNode localeNode,
      @Nonnull Set<String> englishFields) {
    List<String> missingFields = new ArrayList<>();
    for (String field : englishFields) {
      if (!hasCopy(localeNode, field)) {
        missingFields.add(field);
      }
    }

    JsonNode englishFeatures = english.get("features");
    JsonNode localeFeatures = localeNode.get("features");
    if (englishFeatures != null && englishFeatures.isArray() && englishFeatures.size() > 0) {
      if (localeFeatures == null || !localeFeatures.isArray()) {
        missingFields.add("features");
      } else if (localeFeatures.size() != englishFeatures.size()) {
        Assert.fail(
            flavor.jsonPath()
                + " i18n."
                + locale
                + ".features has "
                + localeFeatures.size()
                + " entries but the English features array has "
                + englishFeatures.size()
                + ".");
      } else {
        for (int i = 0; i < englishFeatures.size(); i++) {
          JsonNode englishFeature = englishFeatures.get(i);
          JsonNode localeFeature = localeFeatures.get(i);
          if (localeFeature == null || !localeFeature.isObject()) {
            missingFields.add("features[" + i + "]");
            continue;
          }
          if (hasCopy(englishFeature, "title") && !hasCopy(localeFeature, "title")) {
            missingFields.add("features[" + i + "].title");
          }
          if (hasCopy(englishFeature, "description") && !hasCopy(localeFeature, "description")) {
            missingFields.add("features[" + i + "].description");
          }
          if (hasCopy(englishFeature, "availability") && !hasCopy(localeFeature, "availability")) {
            missingFields.add("features[" + i + "].availability");
          }
        }
      }
    }

    Assert.assertTrue(
        missingFields.isEmpty(),
        flavor.jsonPath()
            + " i18n."
            + locale
            + " is missing translated copy for "
            + missingFields
            + " (present on the English top-level object).");
  }

  private static boolean hasCopy(@Nonnull JsonNode node, @Nonnull String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual()) {
      return false;
    }
    String text = value.asText();
    return !text.isBlank() && !"null".equals(text);
  }

  @Nonnull
  private static Path resolveRepoRoot() {
    String override = System.getProperty("datahub.repoRoot");
    if (override != null && !override.isBlank()) {
      return Path.of(override).toAbsolutePath().normalize();
    }
    Path cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    Path current = cwd;
    while (current != null) {
      if (new File(current.toFile(), "docs/how/updating-datahub.md").isFile()
          && new File(current.toFile(), "metadata-service/configuration").isDirectory()) {
        return current;
      }
      current = current.getParent();
    }
    throw new IllegalStateException(
        "Could not locate DataHub repo root from user.dir="
            + cwd
            + "; set -Ddatahub.repoRoot=/path/to/datahub");
  }

  @Nonnull
  private JsonNode readProductUpdate(@Nonnull ProductUpdateFlavor flavor) throws IOException {
    Path jsonPath = repoRoot.resolve(flavor.jsonPath());
    Assert.assertTrue(Files.isRegularFile(jsonPath), "Missing product update JSON: " + jsonPath);
    return MAPPER.readTree(Files.readString(jsonPath));
  }

  /**
   * The link the toast button actually uses; the parser prefers the primary CTA over the legacy
   * one.
   */
  @Nonnull
  private static String effectiveCtaLink(@Nonnull JsonNode json) {
    if (json.hasNonNull("primaryCtaText") && json.hasNonNull("primaryCtaLink")) {
      return json.get("primaryCtaLink").asText("");
    }
    return json.path("ctaLink").asText("");
  }

  @Nonnull
  private static String staleUpdateMessage(
      @Nonnull ProductUpdateFlavor flavor,
      @Nonnull ReleaseVersion declared,
      @Nonnull ReleaseVersion latest) {
    return String.format(
        "%s shipped %s but %s still advertises %s.\n"
            + "Update its id, description, and CTA link to %s. The hosted product.datahub.com JSON"
            + " is published from this file, so connected and air-gapped instances both show"
            + " whatever it says.\n"
            + "Latest release resolved from %s.",
        flavor.label(), latest, flavor.jsonPath(), declared, latest, flavor.releaseSource());
  }
}
