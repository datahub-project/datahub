package com.linkedin.metadata.sqlsetup.postgres.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;

/** Outcome of a single {@link PostgresSqlMigrationRunner#migrate} invocation. */
@Getter
public final class SqlMigrationResult {

  private final List<String> applied = new ArrayList<>();
  private final List<String> skipped = new ArrayList<>();

  void recordApplied(@Nonnull String version, @Nonnull String scriptName) {
    applied.add(version + " (" + scriptName + ")");
  }

  void recordSkipped(@Nonnull String version, @Nonnull String scriptName) {
    skipped.add(version + " (" + scriptName + ")");
  }

  @Nonnull
  public List<String> getApplied() {
    return Collections.unmodifiableList(applied);
  }

  @Nonnull
  public List<String> getSkipped() {
    return Collections.unmodifiableList(skipped);
  }
}
