package com.linkedin.metadata.sqlsetup.postgres.migration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Descriptor for a versioned SQL migration module (schema namespace, classpath scripts, ledger
 * table, token substitution).
 */
@Getter
public final class SqlMigrationModule {

  @Nonnull private final String migrationNamespace;
  @Nonnull private final String targetSchema;
  @Nonnull private final String classpathLocation;
  @Nonnull private final String ledgerTableName;
  @Nonnull private final Map<String, String> tokenReplacements;
  @Nullable private final MigrationHook preMigrate;
  @Nullable private final MigrationHook postMigrate;
  @Nonnull private final ClassLoader classLoader;

  private SqlMigrationModule(Builder builder) {
    this.migrationNamespace = Objects.requireNonNull(builder.migrationNamespace);
    this.targetSchema = Objects.requireNonNull(builder.targetSchema);
    this.classpathLocation = normalizeLocation(Objects.requireNonNull(builder.classpathLocation));
    this.ledgerTableName = Objects.requireNonNull(builder.ledgerTableName);
    this.tokenReplacements =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.tokenReplacements));
    this.preMigrate = builder.preMigrate;
    this.postMigrate = builder.postMigrate;
    this.classLoader =
        builder.classLoader != null
            ? builder.classLoader
            : SqlMigrationModule.class.getClassLoader();
  }

  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  @FunctionalInterface
  public interface MigrationHook {
    void run(@Nonnull Connection connection) throws SQLException;
  }

  private static String normalizeLocation(String location) {
    String trimmed = location.trim();
    if (trimmed.endsWith("/")) {
      return trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  public static final class Builder {
    private String migrationNamespace;
    private String targetSchema;
    private String classpathLocation;
    private String ledgerTableName;
    private final Map<String, String> tokenReplacements = new LinkedHashMap<>();
    private MigrationHook preMigrate;
    private MigrationHook postMigrate;
    private ClassLoader classLoader;

    @Nonnull
    public Builder migrationNamespace(@Nonnull String migrationNamespace) {
      this.migrationNamespace = migrationNamespace;
      return this;
    }

    @Nonnull
    public Builder targetSchema(@Nonnull String targetSchema) {
      this.targetSchema = targetSchema;
      return this;
    }

    @Nonnull
    public Builder classpathLocation(@Nonnull String classpathLocation) {
      this.classpathLocation = classpathLocation;
      return this;
    }

    @Nonnull
    public Builder ledgerTableName(@Nonnull String ledgerTableName) {
      this.ledgerTableName = ledgerTableName;
      return this;
    }

    @Nonnull
    public Builder tokenReplacement(@Nonnull String token, @Nonnull String value) {
      this.tokenReplacements.put(token, value);
      return this;
    }

    @Nonnull
    public Builder tokenReplacements(@Nonnull Map<String, String> replacements) {
      this.tokenReplacements.clear();
      this.tokenReplacements.putAll(replacements);
      return this;
    }

    @Nonnull
    public Builder preMigrate(@Nullable MigrationHook preMigrate) {
      this.preMigrate = preMigrate;
      return this;
    }

    @Nonnull
    public Builder postMigrate(@Nullable MigrationHook postMigrate) {
      this.postMigrate = postMigrate;
      return this;
    }

    @Nonnull
    public Builder classLoader(@Nullable ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    @Nonnull
    public SqlMigrationModule build() {
      return new SqlMigrationModule(this);
    }
  }
}
