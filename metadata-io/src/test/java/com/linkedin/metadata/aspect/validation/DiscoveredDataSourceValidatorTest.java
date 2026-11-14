package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_EVIDENCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_INFO_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecision;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecisionEvent;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecisionEventArray;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidence;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntry;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntryMap;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceStatus;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceTier;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class DiscoveredDataSourceValidatorTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:discoveredDataSource:mysql");

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

  @Test
  public void testEvidenceCapsEntries() {
    Map<String, DiscoveredDataSourceEvidenceEntry> entries =
        IntStream.rangeClosed(0, DiscoveredDataSourceValidator.MAX_EVIDENCE_ENTRIES)
            .boxed()
            .collect(Collectors.toMap(i -> "lineage.referenced:" + i, i -> entry(1)));
    assertNotNull(
        DiscoveredDataSourceValidator.validateEvidence(
            new DiscoveredDataSourceEvidence()
                .setEvidence(new DiscoveredDataSourceEvidenceEntryMap(entries))));
  }

  @Test
  public void testDecisionCapsHistory() {
    DiscoveredDataSourceDecisionEvent event =
        new DiscoveredDataSourceDecisionEvent()
            .setStatus(DiscoveredDataSourceStatus.PRESENTED)
            .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
            .setTime(1L);
    DiscoveredDataSourceDecision proposed =
        decision(DiscoveredDataSourceStatus.PRESENTED)
            .setHistory(
                new DiscoveredDataSourceDecisionEventArray(
                    IntStream.rangeClosed(0, DiscoveredDataSourceValidator.MAX_HISTORY_EVENTS)
                        .mapToObj(i -> event)
                        .collect(Collectors.toList())));
    assertNotNull(DiscoveredDataSourceValidator.validateDecision(proposed, null));
  }

  /** The pre-commit path compares against the stored aspect and skips deletes and other aspects. */
  @Test
  public void testPreCommitRoutesByAspect() {
    DiscoveredDataSourceValidator validator =
        new DiscoveredDataSourceValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(DiscoveredDataSourceValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());

    ChangeMCP invalidTransition =
        changeMcp(
            DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME,
            ChangeType.UPSERT,
            decision(DiscoveredDataSourceStatus.ACCEPTED),
            decision(DiscoveredDataSourceStatus.NEW));
    ChangeMCP firstDecision =
        changeMcp(
            DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME,
            ChangeType.UPSERT,
            decision(DiscoveredDataSourceStatus.NEW),
            null);
    ChangeMCP corroboratedEvidence =
        changeMcp(
            DISCOVERED_DATA_SOURCE_EVIDENCE_ASPECT_NAME,
            ChangeType.UPSERT,
            evidence(entry(1).setTier(DiscoveredDataSourceTier.CORROBORATED)),
            null);
    ChangeMCP deletion =
        changeMcp(DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME, ChangeType.DELETE, null, null);
    ChangeMCP otherAspect =
        changeMcp(DISCOVERED_DATA_SOURCE_INFO_ASPECT_NAME, ChangeType.UPSERT, null, null);

    assertEquals(
        validator
            .validatePreCommitAspects(
                OperationFingerprint.EMPTY,
                List.of(
                    invalidTransition, firstDecision, corroboratedEvidence, deletion, otherAspect),
                mock(RetrieverContext.class))
            .count(),
        2);
  }

  private static ChangeMCP changeMcp(
      String aspectName, ChangeType changeType, RecordTemplate proposed, RecordTemplate previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(URN);
    when(item.getAspectName()).thenReturn(aspectName);
    when(item.getChangeType()).thenReturn(changeType);
    if (proposed instanceof DiscoveredDataSourceDecision) {
      when(item.getAspect(DiscoveredDataSourceDecision.class))
          .thenReturn((DiscoveredDataSourceDecision) proposed);
    } else if (proposed instanceof DiscoveredDataSourceEvidence) {
      when(item.getAspect(DiscoveredDataSourceEvidence.class))
          .thenReturn((DiscoveredDataSourceEvidence) proposed);
    }
    if (previous instanceof DiscoveredDataSourceDecision) {
      when(item.getPreviousRecordTemplate()).thenReturn(previous);
      when(item.getPreviousAspect(DiscoveredDataSourceDecision.class))
          .thenReturn((DiscoveredDataSourceDecision) previous);
    }
    return item;
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
