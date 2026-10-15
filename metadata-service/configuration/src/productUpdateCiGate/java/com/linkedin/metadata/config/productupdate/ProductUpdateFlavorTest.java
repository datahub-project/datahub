package com.linkedin.metadata.config.productupdate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProductUpdateFlavorTest {

  @Test
  public void testCoreIgnoresDraftHeadingAndOutOfOrderSections() throws IOException {
    Path repoRoot = Files.createTempDirectory("core-release-notes");
    writeFile(
        repoRoot.resolve("docs/how/updating-datahub.md"),
        String.join(
            "\n",
            "# Updating DataHub",
            "## Next",
            "Draft release notes for the upcoming version.",
            "## v1.7.0",
            "## 1.8.0 — LTS",
            "## v1.5.0 (2026-03-24)",
            // Hotfix sections are listed after the minor release they patch.
            "## v1.5.0.7"));

    Assert.assertEquals(
        ProductUpdateFlavor.CORE.latestRelease(repoRoot), ReleaseVersion.parse("v1.8.0").get());
  }

  @Test
  public void testCloudIgnoresNextNote() throws IOException {
    Path repoRoot = Files.createTempDirectory("cloud-release-notes");
    Path releaseNotes = repoRoot.resolve("docs/managed-datahub/release-notes");
    Files.createDirectories(releaseNotes);
    for (String note : new String[] {"next.md", "v_0_3_17.md", "v_2_0_0.md", "v_2_1_0.md"}) {
      Files.writeString(releaseNotes.resolve(note), "");
    }

    Assert.assertEquals(
        ProductUpdateFlavor.CLOUD.latestRelease(repoRoot), ReleaseVersion.parse("v2.1.0").get());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testFailsLoudlyWhenReleaseHistoryIsMissing() throws IOException {
    ProductUpdateFlavor.CORE.latestRelease(Files.createTempDirectory("empty-repo"));
  }

  private static void writeFile(Path path, String contents) throws IOException {
    Files.createDirectories(path.getParent());
    Files.writeString(path, contents);
  }
}
