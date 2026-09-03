package com.linkedin.metadata.config.usage.cigate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class RequestContextInstrumentationScanner {

  private static final Pattern WITH_USAGE_OPERATION =
      Pattern.compile("\\.withUsageOperation\\(|\\.usageOperation\\(");
  private static final Pattern USAGE_OPERATION_ARG = Pattern.compile("UsageOperation\\.([A-Z_]+)");

  private RequestContextInstrumentationScanner() {}

  @Nonnull
  public static HandlerInstrumentationSurface scan(
      @Nonnull Path repoRoot,
      @Nonnull List<String> sourceRootSuffixes,
      @Nonnull String buildMethodMarker,
      @Nonnull Pattern buildMethodPattern,
      @Nonnull List<String> knownWrapperMethods)
      throws IOException {
    List<HandlerEntryBuilder> entries = new ArrayList<>();
    for (String sourceRootSuffix : sourceRootSuffixes) {
      Path sourceRoot = repoRoot.resolve(sourceRootSuffix);
      if (!Files.isDirectory(sourceRoot)) {
        continue;
      }
      try (Stream<Path> paths = Files.walk(sourceRoot)) {
        paths
            .filter(path -> path.toString().endsWith(".java"))
            .forEach(
                path -> {
                  try {
                    scanFile(
                        repoRoot,
                        path,
                        entries,
                        buildMethodMarker,
                        buildMethodPattern,
                        knownWrapperMethods);
                  } catch (IOException e) {
                    throw new IllegalStateException("Failed to scan " + path, e);
                  }
                });
      }
    }
    List<HandlerInstrumentationSurface.HandlerEntry> handlers =
        entries.stream().map(HandlerEntryBuilder::build).toList();
    return new HandlerInstrumentationSurface(handlers);
  }

  private static void scanFile(
      @Nonnull Path repoRoot,
      @Nonnull Path file,
      @Nonnull List<HandlerEntryBuilder> entries,
      @Nonnull String buildMethodMarker,
      @Nonnull Pattern buildMethodPattern,
      @Nonnull List<String> knownWrapperMethods)
      throws IOException {
    String content = Files.readString(file);
    if (!content.contains(buildMethodMarker)) {
      return;
    }
    String relative = repoRoot.relativize(file).toString().replace('\\', '/');
    Matcher matcher = buildMethodPattern.matcher(content);
    while (matcher.find()) {
      int line = lineNumber(content, matcher.start());
      String context = extractBuilderContext(content, matcher.start());
      boolean instrumented = isInstrumented(context, knownWrapperMethods);
      String usageOperation = extractUsageOperation(context);
      entries.add(new HandlerEntryBuilder(relative, line, instrumented, usageOperation));
    }
  }

  private static boolean isInstrumented(
      @Nonnull String context, @Nonnull List<String> knownWrapperMethods) {
    if (WITH_USAGE_OPERATION.matcher(context).find()) {
      return true;
    }
    for (String wrapper : knownWrapperMethods) {
      if (context.contains(wrapper)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private static String extractUsageOperation(@Nonnull String context) {
    Matcher matcher = USAGE_OPERATION_ARG.matcher(context);
    if (matcher.find()) {
      return matcher.group(1).toLowerCase(Locale.ROOT);
    }
    return null;
  }

  @Nonnull
  private static String extractBuilderContext(@Nonnull String content, int buildMethodIndex) {
    int builderStart = content.lastIndexOf("RequestContext.builder()", buildMethodIndex);
    if (builderStart < 0) {
      builderStart = Math.max(0, buildMethodIndex - 400);
    }
    int end = Math.min(content.length(), buildMethodIndex + 800);
    int asSessionEnd = content.indexOf("OperationContext.asSession", builderStart);
    if (asSessionEnd >= 0) {
      int closing = findMatchingParen(content, content.indexOf('(', asSessionEnd));
      if (closing > buildMethodIndex) {
        end = Math.min(end, closing + 1);
      }
    }
    return content.substring(builderStart, end);
  }

  private static int findMatchingParen(@Nonnull String content, int openParenIndex) {
    int depth = 0;
    for (int i = openParenIndex; i < content.length(); i++) {
      char c = content.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return content.length() - 1;
  }

  private static int lineNumber(@Nonnull String content, int index) {
    int line = 1;
    for (int i = 0; i < index && i < content.length(); i++) {
      if (content.charAt(i) == '\n') {
        line++;
      }
    }
    return line;
  }

  private record HandlerEntryBuilder(
      String sourceFile, int lineNumber, boolean instrumented, @Nullable String usageOperation) {

    @Nonnull
    HandlerInstrumentationSurface.HandlerEntry build() {
      return new HandlerInstrumentationSurface.HandlerEntry(
          sourceFile, lineNumber, instrumented, usageOperation);
    }
  }
}
