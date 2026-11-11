package com.linkedin.metadata.aspect.validation;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecision;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidence;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntry;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntryMap;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceStatus;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceTier;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class DiscoveredDataSourceValidatorTest {

  @Test
  public void testAllowedTransitions() {
    assertNull(validate(DiscoveredDataSourceStatus.PRESENTED, DiscoveredDataSourceStatus.NEW));
    assertNull(
        validate(
            declined(DiscoveredDataSourceTier.FINGERPRINT),
            decision(DiscoveredDataSourceStatus.PRESENTED)));
    // A declined source comes back as NEW once its evidence tier rises.
    assertNull(
        validate(
            decision(DiscoveredDataSourceStatus.NEW),
            declined(DiscoveredDataSourceTier.FINGERPRINT)));
    assertNull(validate(DiscoveredDataSourceStatus.CONNECTED, DiscoveredDataSourceStatus.DECLINED));
  }

  @Test
  public void testRejectsSkippingPresentation() {
    assertNotNull(validate(DiscoveredDataSourceStatus.ACCEPTED, DiscoveredDataSourceStatus.NEW));
    assertNotNull(
        validate(
            declined(DiscoveredDataSourceTier.FINGERPRINT),
            decision(DiscoveredDataSourceStatus.NEW)));
  }

  @Test
  public void testDeclineAndSnoozeRequireTheirFields() {
    assertNotNull(
        DiscoveredDataSourceValidator.validateDecision(
            decision(DiscoveredDataSourceStatus.DECLINED),
            decision(DiscoveredDataSourceStatus.PRESENTED)));
    assertNotNull(
        DiscoveredDataSourceValidator.validateDecision(
            decision(DiscoveredDataSourceStatus.SNOOZED),
            decision(DiscoveredDataSourceStatus.PRESENTED)));
    assertNull(
        DiscoveredDataSourceValidator.validateDecision(
            decision(DiscoveredDataSourceStatus.SNOOZED).setSnoozeUntil(1L),
            decision(DiscoveredDataSourceStatus.PRESENTED)));
  }

  @Test
  public void testEvidenceRejectsReportedCorroboratedTier() {
    assertNull(DiscoveredDataSourceValidator.validateEvidence(evidence(entry(1))));
    DiscoveredDataSourceEvidenceEntry corroborated =
        entry(1).setTier(DiscoveredDataSourceTier.CORROBORATED);
    assertNotNull(DiscoveredDataSourceValidator.validateEvidence(evidence(corroborated)));
  }

  @Test
  public void testEvidenceCapsSampleUrns() {
    assertNotNull(
        DiscoveredDataSourceValidator.validateEvidence(
            evidence(entry(DiscoveredDataSourceValidator.MAX_SAMPLE_URNS + 1))));
  }

  private static String validate(
      DiscoveredDataSourceStatus proposed, DiscoveredDataSourceStatus previous) {
    return validate(decision(proposed), decision(previous));
  }

  private static String validate(
      DiscoveredDataSourceDecision proposed, DiscoveredDataSourceDecision previous) {
    return DiscoveredDataSourceValidator.validateDecision(proposed, previous);
  }

  private static DiscoveredDataSourceDecision decision(DiscoveredDataSourceStatus status) {
    return new DiscoveredDataSourceDecision().setStatus(status);
  }

  private static DiscoveredDataSourceDecision declined(DiscoveredDataSourceTier tier) {
    return decision(DiscoveredDataSourceStatus.DECLINED).setTierAtDecision(tier);
  }

  private static DiscoveredDataSourceEvidenceEntry entry(int sampleCount) {
    return new DiscoveredDataSourceEvidenceEntry()
        .setDetector("lineage.referenced")
        .setTier(DiscoveredDataSourceTier.FINGERPRINT)
        .setLastSeen(1L)
        .setSampleUrns(
            new UrnArray(
                IntStream.range(0, sampleCount)
                    .mapToObj(
                        i ->
                            UrnUtils.getUrn(
                                "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table_"
                                    + i
                                    + ",PROD)"))
                    .collect(Collectors.toList())));
  }

  private static DiscoveredDataSourceEvidence evidence(DiscoveredDataSourceEvidenceEntry entry) {
    return new DiscoveredDataSourceEvidence()
        .setEvidence(
            new DiscoveredDataSourceEvidenceEntryMap(
                Map.of("lineage.referenced:urn:li:dataHubIngestionSource:a", entry)));
  }
}
