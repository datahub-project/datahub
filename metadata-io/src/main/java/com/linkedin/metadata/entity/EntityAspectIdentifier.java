package com.linkedin.metadata.entity;

import com.linkedin.metadata.entity.cassandra.CassandraAspect;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This class holds values required to construct a unique key to identify an entity aspect record in
 * a database. Its existence started mainly for compatibility with {@link
 * com.linkedin.metadata.entity.ebean.EbeanAspectV2.PrimaryKey}
 */
@Value
@Slf4j
public class EntityAspectIdentifier {
  @Nonnull String urn;
  @Nonnull String aspect;
  long version;

  public static EntityAspectIdentifier fromEbean(EbeanAspectV2 ebeanAspectV2) {
    return new EntityAspectIdentifier(
        ebeanAspectV2.getUrn(), ebeanAspectV2.getAspect(), ebeanAspectV2.getVersion());
  }

  public static EntityAspectIdentifier fromCassandra(CassandraAspect cassandraAspect) {
    return new EntityAspectIdentifier(
        cassandraAspect.getUrn(), cassandraAspect.getAspect(), cassandraAspect.getVersion());
  }
}
