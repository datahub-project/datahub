package com.linkedin.metadata.sqlsetup.postgres.migration;

import javax.annotation.Nonnull;
import lombok.Value;

/** Parsed classpath migration script metadata. */
@Value
public class SqlMigrationScript implements Comparable<SqlMigrationScript> {

  @Nonnull String version;
  @Nonnull String description;
  @Nonnull SqlMigrationType type;
  @Nonnull String scriptName;
  @Nonnull String classpathLocation;
  int versionRank;

  @Override
  public int compareTo(@Nonnull SqlMigrationScript other) {
    int typeOrder = Integer.compare(type.ordinal(), other.type.ordinal());
    if (typeOrder != 0) {
      return typeOrder;
    }
    return Integer.compare(versionRank, other.versionRank);
  }
}
