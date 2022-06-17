package com.linkedin.metadata;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;


public class AspectGenerationUtils {

  private AspectGenerationUtils() {
  }

  @Nonnull
  public static AuditStamp createAuditStamp() {
    return new AuditStamp().setTime(123L).setActor(UrnUtils.getUrn("urn:li:corpuser:tester"));
  }

  @Nonnull
  public static SystemMetadata createSystemMetadata() {
    SystemMetadata metadata = new SystemMetadata();
    metadata.setLastObserved(1625792689);
    metadata.setRunId("run-123");
    return metadata;
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
  public static String getAspectName(RecordTemplate record) {
    return PegasusUtils.getAspectNameFromSchema(record.schema());
  }
}
