package com.linkedin.metadata.config.usage.cigate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public record HandlerInstrumentationSurface(@Nonnull List<HandlerEntry> handlers) {

  public HandlerInstrumentationSurface {
    handlers = List.copyOf(new ArrayList<>(handlers));
  }

  @Nonnull
  Set<String> keys() {
    Set<String> keys = new TreeSet<>();
    for (HandlerEntry handler : handlers) {
      keys.add(handler.key());
    }
    return keys;
  }

  @Nonnull
  Map<String, HandlerEntry> handlersByKey() {
    Map<String, HandlerEntry> byKey = new LinkedHashMap<>();
    for (HandlerEntry handler : handlers) {
      byKey.put(handler.key(), handler);
    }
    return Map.copyOf(byKey);
  }

  @Nonnull
  public HandlerInstrumentationSurface merge(@Nonnull HandlerInstrumentationSurface overlay) {
    List<HandlerEntry> merged = new ArrayList<>(handlers);
    Set<String> keys = keys();
    for (HandlerEntry handler : overlay.handlers) {
      if (!keys.contains(handler.key())) {
        merged.add(handler);
      }
    }
    return new HandlerInstrumentationSurface(merged);
  }

  /**
   * Handlers that are not instrumented and lack a documented exemption in the snapshot (or profile
   * override).
   */
  @Nonnull
  public List<HandlerEntry> uninstrumentedWithoutExemption(
      @Nonnull HandlerExemptionSnapshot exemptions,
      @Nonnull Predicate<HandlerEntry> profileExempt) {
    List<HandlerEntry> untagged = new ArrayList<>();
    for (HandlerEntry handler : handlers) {
      if (handler.instrumented()) {
        continue;
      }
      if (exemptions.exemptionReasonFor(handler.sourceFile()) != null) {
        continue;
      }
      if (profileExempt.test(handler)) {
        continue;
      }
      untagged.add(handler);
    }
    return untagged;
  }

  /** First uninstrumented scan entry for a source file, if any. */
  @Nullable
  public HandlerEntry uninstrumentedEntryForSourceFile(@Nonnull String sourceFile) {
    for (HandlerEntry handler : handlers) {
      if (handler.sourceFile().equals(sourceFile) && !handler.instrumented()) {
        return handler;
      }
    }
    return null;
  }

  @Nonnull
  public String toJson(@Nonnull ObjectMapper mapper) throws IOException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Nonnull
  public static HandlerInstrumentationSurface fromJsonPath(
      @Nonnull Path path, @Nonnull ObjectMapper mapper) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return mapper.readValue(in, HandlerInstrumentationSurface.class);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record HandlerEntry(
      @Nonnull String sourceFile,
      int lineNumber,
      boolean instrumented,
      @Nullable String usageOperation) {

    @Nonnull
    String key() {
      return sourceFile + ":" + lineNumber;
    }
  }
}
