package com.linkedin.metadata.config.productupdate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryRepoPaths;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 */
public class ProductUpdateReleaseSyncTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Mirrors ProductUpdateParser's required-field contract; the toast never renders if absent. */
  private static final List<String> REQUIRED_FIELDS = List.of("enabled", "id", "title");

  private static final Pattern BUNDLED_IMAGE =
      Pattern.compile("https://raw\\.githubusercontent\\.com/datahub-project/datahub/[^/]+/(.+)");

  private Path repoRoot;

  @BeforeClass
  public void setUp() {
    repoRoot = UsageRegistryRepoPaths.repoRoot();
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
