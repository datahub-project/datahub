package com.linkedin.metadata.entity.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import com.linkedin.metadata.entity.EntityAspect;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.sql.Timestamp;

// Dumb object for now

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CassandraAspect implements EntityAspect {

  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode
  public static class PrimaryKey {

    private String urn;
    private String aspect;
    private long version;

    public static PrimaryKey fromRow(Row r) {
      return new CassandraAspect.PrimaryKey(
          r.getString(CassandraAspect.URN_COLUMN),
          r.getString(CassandraAspect.ASPECT_COLUMN),
          r.getLong(CassandraAspect.VERSION_COLUMN));
    }
  }

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

  public CassandraAspect.PrimaryKey toPrimaryKey() {
    return new PrimaryKey(getUrn(), getAspect(), getVersion());
  }

  public static CassandraAspect fromRow(@Nonnull Row r) {
    return new CassandraAspect(
        r.getString(CassandraAspect.URN_COLUMN),
        r.getString(CassandraAspect.ASPECT_COLUMN),
        r.getLong(CassandraAspect.VERSION_COLUMN),
        r.getString(CassandraAspect.METADATA_COLUMN),
        r.getString(CassandraAspect.SYSTEM_METADATA_COLUMN),
        r.getInstant(CassandraAspect.CREATED_ON_COLUMN) == null ? null : Timestamp.from(r.getInstant(CassandraAspect.CREATED_ON_COLUMN)),
        r.getString(CassandraAspect.CREATED_BY_COLUMN),
        r.getString(CassandraAspect.CREATED_FOR_COLUMN));
  }
}