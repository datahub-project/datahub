package com.linkedin.metadata;

import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AspectGenerationUtils {

  private AspectGenerationUtils() {}

  @Nonnull
  public static AuditStamp createAuditStamp() {
    return new AuditStamp().setTime(123L).setActor(UrnUtils.getUrn("urn:li:corpuser:tester"));
  }

  @Nonnull
  public static SystemMetadata createSystemMetadata() {
    return createSystemMetadata(1625792689, "run-123");
  }

  @Nonnull
  public static SystemMetadata createSystemMetadata(int nextAspectVersion) {
    return createSystemMetadata(
        1625792689, "run-123", "run-123", String.valueOf(nextAspectVersion));
  }

  @Nonnull
  public static SystemMetadata createSystemMetadata(int lastObserved, @Nonnull String runId) {
    return createSystemMetadata(lastObserved, runId, runId, null);
  }

  @Nonnull
  public static SystemMetadata createSystemMetadata(
      int lastObserved, // for test comparison must be int
      @Nonnull String runId,
      @Nonnull String lastRunId,
      @Nullable String version) {
    SystemMetadata metadata = createDefaultSystemMetadata(runId);
    metadata.setLastRunId(lastRunId);
    metadata.setVersion(version, SetMode.IGNORE_NULL);
    metadata.setLastObserved(lastObserved);
    return metadata;
  }

  @Nonnull
  public static CorpUserKey createCorpUserKey(Urn urn) {
    return (CorpUserKey)
        EntityKeyUtils.convertUrnToEntityKeyInternal(urn, new CorpUserKey().schema());
  }

  @Nonnull
  public static CorpUserInfo createCorpUserInfo(@Nonnull String email) {
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setEmail(email);
    corpUserInfo.setActive(true);
    return corpUserInfo;
  }

  @Nonnull
  public static ChartInfo createChartInfo(@Nonnull String title, @Nonnull String description) {
    ChartInfo chartInfo = new ChartInfo();
    chartInfo.setTitle(title);
    chartInfo.setDescription(description);
    ChangeAuditStamps lastModified = new ChangeAuditStamps();
    lastModified.setCreated(createAuditStamp());
    lastModified.setLastModified(createAuditStamp());
    chartInfo.setLastModified(lastModified);
    return chartInfo;
  }

  @Nonnull
  public static UpstreamLineage createUpstreamLineage() {
    final UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreams = new UpstreamArray();
    upstreamLineage.setUpstreams(upstreams);
    return upstreamLineage;
  }

  @Nonnull
  public static String getAspectName(RecordTemplate record) {
    return PegasusUtils.getAspectNameFromSchema(record.schema());
  }
}
