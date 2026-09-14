package com.linkedin.gms.factory.aws;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * GMS modules must not call AWS SDK v2 client builders without {@code credentialsProvider} (SDK
 * 2.30 IRSA default chain leaks {@code StsAssumeRoleWithWebIdentityCredentialsProvider}).
 */
public class AwsAuthEfficiencyTest {

  private static final Pattern CLIENT_BUILDER =
      Pattern.compile(
          "(S3Client|StsClient|SqsClient|S3Presigner|BedrockRuntimeClient|EventBridgeClient)\\.builder\\(\\)");

  private static final Pattern NO_ARG_S3_FILE_IO = Pattern.compile("new S3FileIO\\(\\s*\\)");

  private static final Pattern CREDENTIALS_PROVIDER = Pattern.compile("credentialsProvider\\s*\\(");

  private static final int LOOKAHEAD_LINES = 24;

  private static final List<Path> SOURCE_ROOTS =
      List.of(
          Path.of("src/main/java"),
          Path.of("../../metadata-io/src/main/java"),
          Path.of("../../datahub-graphql-core/src/main/java"),
          Path.of("../iceberg-catalog/src/main/java"),
          Path.of("../../metadata-utils/src/main/java"));

  @Test
  public void awsClientBuildersSetCredentialsProvider() throws IOException {
    int filesScanned = 0;
    for (Path root : SOURCE_ROOTS) {
      assertTrue(Files.isDirectory(root), "missing source root " + root.toAbsolutePath());
      try (Stream<Path> paths = Files.walk(root)) {
        List<Path> javaFiles = paths.filter(path -> path.toString().endsWith(".java")).toList();
        filesScanned += javaFiles.size();
        for (Path path : javaFiles) {
          assertBuildersHaveCredentials(path);
          assertNoArgS3FileIoBanned(path);
        }
      }
    }
    assertTrue(filesScanned > 100, "expected to scan GMS Java sources, scanned " + filesScanned);
  }

  private static void assertBuildersHaveCredentials(Path path) throws IOException {
    List<String> lines = Files.readAllLines(path);
    for (int i = 0; i < lines.size(); i++) {
      Matcher matcher = CLIENT_BUILDER.matcher(lines.get(i));
      if (!matcher.find()) {
        continue;
      }
      String window = window(lines, i, LOOKAHEAD_LINES);
      if (!CREDENTIALS_PROVIDER.matcher(window).find()) {
        fail(
            path
                + ":"
                + (i + 1)
                + " AWS client builder without credentialsProvider() in the next "
                + LOOKAHEAD_LINES
                + " lines");
      }
    }
  }

  private static void assertNoArgS3FileIoBanned(Path path) throws IOException {
    String source = Files.readString(path);
    if (NO_ARG_S3_FILE_IO.matcher(source).find()) {
      fail(path + " uses no-arg new S3FileIO() (Iceberg default IRSA chain)");
    }
  }

  private static String window(List<String> lines, int start, int lookahead) {
    int end = Math.min(lines.size(), start + lookahead);
    StringBuilder builder = new StringBuilder();
    for (int i = start; i < end; i++) {
      builder.append(lines.get(i)).append('\n');
    }
    return builder.toString();
  }
}
