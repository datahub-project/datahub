package com.linkedin.gms.factory.aws;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * Guards against reintroducing inline AWS credential/client creation in GMS factories that can
 * orphan IRSA credential refresh tasks.
 */
public class AwsAuthEfficiencyTest {

  private static final Path FACTORIES_MAIN = Path.of("src/main/java/com/linkedin/gms/factory");

  private static final List<Pattern> BANNED_PATTERNS =
      List.of(
          Pattern.compile("DefaultCredentialsProvider\\.create\\(\\)"),
          Pattern.compile("StsClient\\.create\\(\\)"),
          Pattern.compile("S3Client\\.builder\\(\\)"),
          Pattern.compile("S3Client\\.create\\(\\)"),
          Pattern.compile("S3Presigner\\.builder\\(\\)"),
          Pattern.compile("S3Presigner\\.create\\(\\)"));

  private static final List<String> ALLOWED_FILES =
      List.of("aws/AwsClientFactory.java", "s3/StsClientFactory.java");

  @Test
  public void factoriesDoNotCreateInlineAwsCredentialProviders() throws IOException {
    assertTrue(Files.isDirectory(FACTORIES_MAIN), "Expected factories main source directory");

    try (Stream<Path> paths = Files.walk(FACTORIES_MAIN)) {
      paths
          .filter(path -> path.toString().endsWith(".java"))
          .forEach(
              path -> {
                String relative = FACTORIES_MAIN.relativize(path).toString().replace('\\', '/');
                if (ALLOWED_FILES.contains(relative)) {
                  return;
                }
                try {
                  String source = Files.readString(path);
                  for (Pattern pattern : BANNED_PATTERNS) {
                    assertFalse(
                        pattern.matcher(source).find(),
                        "Banned AWS pattern " + pattern.pattern() + " found in " + relative);
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
