package com.linkedin.common.urn;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UrnUtils {

  private static final CorpuserUrn UNKNOWN_ACTOR_URN = new CorpuserUrn("unknown");

  private UrnUtils() {}

  /**
   * Convert platform + dataset + origin into DatasetUrn
   *
   * @param platformName String, e.g. hdfs, oracle
   * @param datasetName String, e.g. /jobs/xxx, ABOOK.ADDRESS
   * @param origin PROD, CORP, EI, DEV
   * @return DatasetUrn
   */
  @Nonnull
  public static DatasetUrn toDatasetUrn(
      @Nonnull String platformName, @Nonnull String datasetName, @Nonnull String origin) {
    return new DatasetUrn(
        new DataPlatformUrn(platformName), datasetName, FabricType.valueOf(origin.toUpperCase()));
  }

  public static Urn getUrn(String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve entity with urn %s, invalid urn", urnStr));
    }
  }

  /**
   * Parses a required URN string, rejecting null/blank values and surfacing parse failures as
   * {@link IllegalArgumentException} for API layers that map client errors to 4xx responses.
   */
  @Nonnull
  public static Urn requireUrn(@Nullable String urnStr) {
    if (urnStr == null || urnStr.isBlank()) {
      throw new IllegalArgumentException("URN must not be null or empty");
    }
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Failed to parse URN %s: %s", urnStr, e.getMessage()), e);
    }
  }

  /**
   * Get audit stamp without time. If actor is null, set as Unknown Application URN.
   *
   * @param actor Urn
   * @return AuditStamp
   */
  @Nonnull
  public static AuditStamp getAuditStamp(@Nullable Urn actor) {
    return new AuditStamp().setActor(getActorOrDefault(actor));
  }

  /** Return actor URN, if input actor is null, return Unknown Application URN. */
  @Nonnull
  public static Urn getActorOrDefault(@Nullable Urn actor) {
    return actor != null ? actor : UNKNOWN_ACTOR_URN;
  }
}
