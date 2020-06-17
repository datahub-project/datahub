package com.linkedin.metadata.utils;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipSuggestion;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;


public class TestUtils {

  private TestUtils() {
    // Util class
  }

  @Nonnull
  public static CorpuserUrn makeCorpUserUrn(@Nonnull String name) {
    return new CorpuserUrn(name);
  }

  @Nonnull
  public static DatasetUrn makeDatasetUrn(@Nonnull String name) {
    return new DatasetUrn(new DataPlatformUrn("mysql"), name, FabricType.DEV);
  }

  @Nonnull
  public static DataProcessUrn makeDataProcessUrn(@Nonnull String name) {
    return new DataProcessUrn("Azure Data Factory", name, FabricType.DEV);
  }

  @Nonnull
  public static Owner makeOwner(@Nonnull String ldap) {
    return makeOwner(ldap, OwnershipType.DEVELOPER);
  }

  @Nonnull
  public static Owner makeOwner(@Nonnull String ldap, @Nonnull OwnershipType type) {
    return new Owner().setOwner(new CorpuserUrn(ldap)).setType(type);
  }

  @Nonnull
  public static Owner makeOwner(@Nonnull Urn ownerUrn, @Nonnull OwnershipType type,
      @Nullable OwnershipSourceType sourceType, @Nullable String sourceUrl) {
    Owner owner = new Owner().setOwner(ownerUrn).setType(type);

    if (sourceType != null) {
      OwnershipSource source = new OwnershipSource().setType(sourceType);
      if (sourceUrl != null) {
        source.setUrl(sourceUrl);
      }
      owner.setSource(source);
    }

    return owner;
  }

  @Nonnull
  public static Status makeStatus(@Nonnull boolean removedStatus) {
    return new Status().setRemoved(removedStatus);
  }

  @Nonnull
  public static Ownership makeOwnership(@Nonnull String ldap) {
    return new Ownership().setOwners(new OwnerArray(Collections.singleton(makeOwner(ldap))));
  }

  @Nonnull
  public static OwnershipSuggestion makeOwnershipSuggestion(@Nonnull String ldap) {
    return new OwnershipSuggestion().setOwners(new OwnerArray(Collections.singleton(makeOwner(ldap))));
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull Urn actorUrn, @Nullable Urn impersonatorUrn, long time) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(time);
    auditStamp.setActor(actorUrn);
    if (impersonatorUrn != null) {
      auditStamp.setImpersonator(impersonatorUrn);
    }
    return auditStamp;
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull String actorLdap, long time) {
    return makeAuditStamp(new CorpuserUrn(actorLdap), null, time);
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull String actorLdap) {
    return makeAuditStamp(actorLdap, 0L);
  }

  @Nonnull
  public static String loadJsonFromResource(@Nonnull String resourceName) throws IOException {
    return IOUtils.toString(ClassLoader.getSystemResourceAsStream(resourceName), Charset.defaultCharset());
  }
}