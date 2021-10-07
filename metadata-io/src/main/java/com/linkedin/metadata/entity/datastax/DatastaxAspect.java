package com.linkedin.metadata.entity.datastax;

import com.linkedin.metadata.entity.EntityAspect;

import java.sql.Timestamp;

// Dumb object for now

public class DatastaxAspect implements EntityAspect {
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
    return String.format("urn: %s, aspect: %s, version: %s, metadata: %s, createdon: %s, createdby: %s, createdfor: %s, systemmetadata: %s",
                         urn, aspect, version, metadata, createdOn, createdBy, createdFor, systemMetadata
                  );
  }

  public static class PrimaryKey {
    private String urn;
    private String aspect;
    private long version;

    public PrimaryKey(String urn, String aspect, long version) {
      setUrn(urn);
      setAspect(aspect);
      setVersion(version);
    }

    public String getUrn() {
      return urn;
    }

    public void setUrn(String urn) {
      this.urn = urn;
    }

    public String getAspect() {
      return aspect;
    }

    public void setAspect(String aspect) {
      this.aspect = aspect;
    }

    public long getVersion() {
      return version;
    }

    public void setVersion(long version) {
      this.version = version;
    }
  }

  public PrimaryKey toPrimaryKey() {
    return new PrimaryKey(this.urn, this.aspect, this.version);
  }

  public DatastaxAspect(String urn,
                        String aspect,
                        long version,
                        String metadata,
                        String systemMetadata,
                        Timestamp createdOn,
                        String createdBy,
                        String createdFor) {
    this.urn = urn;
    this.aspect = aspect;
    this.version = version;
    this.metadata = metadata;
    this.systemMetadata = systemMetadata;
    this.createdOn = createdOn;
    this.createdBy = createdBy;
    this.createdFor = createdFor;
  }

  public String getCreatedFor() {
    return createdFor;
  }

  public void setCreatedFor(String createdFor) {
    this.createdFor = createdFor;
  }

  public String getAspect() {
    return aspect;
  }

  public void setAspect(String aspect) {
    this.aspect = aspect;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public String getMetadata() {
    return metadata;
  }

  public void setMetadata(String metadata) {
    this.metadata = metadata;
  }

  public String getSystemMetadata() {
    return systemMetadata;
  }

  public void setSystemMetadata(String systemMetadata) {
    this.systemMetadata = systemMetadata;
  }

  public Timestamp getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(Timestamp createdOn) {
    this.createdOn = createdOn;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getUrn() {
    return urn;
  }

  public void setUrn(String urn) {
    this.urn = urn;
  }
}