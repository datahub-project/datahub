package com.linkedin.metadata.entity.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import java.sql.Timestamp;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * This class represents entity aspect records stored in Cassandra database. It's also aware of
 * {@link EntityAspect} which is a shared in-memory representation of an aspect record and knows how
 * to translate itself to it.
 *
 * <p>TODO: Consider using datastax java driver `@Entity` (see:
 * https://docs.datastax.com/en/developer/java-driver/4.13/manual/mapper/entities/)
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CassandraAspect {

  private String urn;
  private String aspect;
  private long version;
  private String metadata;
  private String systemMetadata;
  private Timestamp createdOn;
  private String createdBy;
  private String createdFor;

  public static final String TABLE_NAME = "metadata_aspect_v2";

  public static final String URN_COLUMN = "urn";
  public static final String ASPECT_COLUMN = "aspect";
  public static final String VERSION_COLUMN = "version";
  public static final String METADATA_COLUMN = "metadata";
  public static final String CREATED_ON_COLUMN = "createdon";
  public static final String CREATED_BY_COLUMN = "createdby";
  public static final String CREATED_FOR_COLUMN = "createdfor";
  public static final String SYSTEM_METADATA_COLUMN = "systemmetadata";

  public static final String ENTITY_COLUMN = "entity";

  public String toString() {
    return String.format(
        "urn: %s, aspect: %s, version: %s, metadata: %s, createdon: %s, createdby: %s, createdfor: %s, systemmetadata: %s",
        urn, aspect, version, metadata, createdOn, createdBy, createdFor, systemMetadata);
  }

  @Nonnull
  public static EntityAspect rowToEntityAspect(@Nonnull Row row) {
    return new EntityAspect(
        row.getString(CassandraAspect.URN_COLUMN),
        row.getString(CassandraAspect.ASPECT_COLUMN),
        row.getLong(CassandraAspect.VERSION_COLUMN),
        row.getString(CassandraAspect.METADATA_COLUMN),
        row.getString(CassandraAspect.SYSTEM_METADATA_COLUMN),
        row.getInstant(CassandraAspect.CREATED_ON_COLUMN) == null
            ? null
            : Timestamp.from(row.getInstant(CassandraAspect.CREATED_ON_COLUMN)),
        row.getString(CassandraAspect.CREATED_BY_COLUMN),
        row.getString(CassandraAspect.CREATED_FOR_COLUMN));
  }

  @Nonnull
  public static EntityAspectIdentifier rowToAspectIdentifier(@Nonnull Row row) {
    return new EntityAspectIdentifier(
        row.getString(CassandraAspect.URN_COLUMN),
        row.getString(CassandraAspect.ASPECT_COLUMN),
        row.getLong(CassandraAspect.VERSION_COLUMN));
  }
}
