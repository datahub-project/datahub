package com.linkedin.metadata.config.usage.cigate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Reviewable list of intentionally uninstrumented {@code buildOpenapi}/{@code buildRestli} sites.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HandlerExemptionSnapshot(@Nonnull List<Exemption> exemptions) {

  public HandlerExemptionSnapshot {
    exemptions = List.copyOf(new ArrayList<>(exemptions));
  }

  @Nonnull
  public HandlerExemptionSnapshot merge(@Nonnull HandlerExemptionSnapshot overlay) {
    Map<String, Exemption> merged = new LinkedHashMap<>();
    for (Exemption exemption : exemptions) {
      merged.put(exemption.sourceFile(), exemption);
    }
    for (Exemption exemption : overlay.exemptions) {
      merged.putIfAbsent(exemption.sourceFile(), exemption);
    }
    return new HandlerExemptionSnapshot(List.copyOf(merged.values()));
  }

  @Nonnull
  Set<String> sourceFiles() {
    Set<String> files = new TreeSet<>();
    for (Exemption exemption : exemptions) {
      files.add(exemption.sourceFile());
    }
    return files;
  }

  /** Exemption documented for this source file (tolerates line-number drift). */
  @Nullable
  public String exemptionReasonFor(@Nonnull String sourceFile) {
    for (Exemption exemption : exemptions) {
      if (exemption.sourceFile().equals(sourceFile)) {
        return exemption.exemptionReason();
      }
    }
    return null;
  }

  @Nonnull
  public String toJson(@Nonnull ObjectMapper mapper) throws IOException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Nonnull
  public static HandlerExemptionSnapshot empty() {
    return new HandlerExemptionSnapshot(List.of());
  }

  @Nonnull
  public static HandlerExemptionSnapshot fromJsonPath(
      @Nonnull Path path, @Nonnull ObjectMapper mapper) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return mapper.readValue(in, HandlerExemptionSnapshot.class);
    }
  }

  /**
   * Reconcile exemptions against a fresh scan: keep documented exemptions that are still
   * uninstrumented, refresh line numbers, and drop stale entries that are now instrumented.
   */
  @Nonnull
  public static HandlerExemptionSnapshot reconcile(
      @Nonnull HandlerInstrumentationSurface scanned, @Nonnull HandlerExemptionSnapshot existing) {
    List<Exemption> reconciled = new ArrayList<>();
    for (Exemption exemption : existing.exemptions) {
      HandlerInstrumentationSurface.HandlerEntry current =
          scanned.uninstrumentedEntryForSourceFile(exemption.sourceFile());
      if (current != null) {
        reconciled.add(exemption.withLineNumber(current.lineNumber()));
      }
    }
    return new HandlerExemptionSnapshot(reconciled);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Exemption(
      @Nonnull String sourceFile,
      @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Integer lineNumber,
      @Nonnull String exemptionReason) {

    @Nonnull
    Exemption withLineNumber(int lineNumber) {
      return new Exemption(sourceFile, lineNumber, exemptionReason);
    }
  }
}
