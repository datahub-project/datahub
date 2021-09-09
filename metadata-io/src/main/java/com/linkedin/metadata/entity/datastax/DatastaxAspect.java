package com.linkedin.metadata.entity.datastax;

import java.sql.Timestamp;

// Dumb object for now

public class DatastaxAspect {
  private String urn;
  private String aspect;
  private long version;
  private String metadata;
  private String systemmetadata;
  private Timestamp createdon;
  private String createdby;
  private String createdfor;

  public String toString() {
    return String.format("urn: %s, aspect: %s, version: %s, metadata: %s, createdon: %s, createdby: %s, createdfor: %s, systemmetadata: %s",
                         urn, aspect, version, metadata, createdon, createdby, createdfor, systemmetadata
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

  public DatastaxAspect() {
  }

  public PrimaryKey toPrimaryKey() {
    return new PrimaryKey(this.urn, this.aspect, this.version);
  }

  public DatastaxAspect(String _urn, String _aspect, long _version, String _metadata, String _systemmetadata, Timestamp _createdon, String _createdby, String _createdfor) {
    urn = _urn;
    aspect = _aspect;
    version = _version;
    metadata = _metadata;
    systemmetadata = _systemmetadata;
    createdon = _createdon;
    createdby = _createdby;
    createdfor = _createdfor;
  }

  public String getCreatedfor() {
    return createdfor;
  }

  public void setCreatedfor(String createdfor) {
    this.createdfor = createdfor;
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

  public String getSystemmetadata() {
    return systemmetadata;
  }

  public void setSystemmetadata(String systemmetadata) {
    this.systemmetadata = systemmetadata;
  }

  public Timestamp getCreatedon() {
    return createdon;
  }

  public void setCreatedon(Timestamp createdon) {
    this.createdon = createdon;
  }

  public String getCreatedby() {
    return createdby;
  }

  public void setCreatedby(String createdby) {
    this.createdby = createdby;
  }

  public String getUrn() {
    return urn;
  }

  public void setUrn(String urn) {
    this.urn = urn;
  }
}