package com.linkedin.metadata.entity.semantic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SemanticNoOpComparatorTest {
  private static final String IGNORE_STAMP =
      "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp\",\"strategy\":\"IGNORE\"}]";
  private static final String IGNORE_TIME =
      "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp/time\","
          + "\"strategy\":\"IGNORE\"}]";
  private static final String WINDOW_TIME =
      "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp/time\","
          + "\"strategy\":\"TIMESTAMP_WINDOW\",\"maxDeltaMs\":1000}]";
  private static final String IGNORE_CREATED =
      "[{\"aspect\":\"assertionInfo\",\"path\":\"/source/created\",\"strategy\":\"IGNORE\"}]";

  private EntityRegistry registry;
  private Urn upstreamA;
  private Urn upstreamB;
  private Urn asserted;

  @BeforeClass
  public void setup() throws Exception {
    registry = TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
    upstreamA = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.a,PROD)");
    upstreamB = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.b,PROD)");
    asserted = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.events,PROD)");
  }

  @Test
  public void disabledAndUnruledAspectsDoNotWalk() {
    SemanticNoOpComparator disabled =
        SemanticNoOpComparator.compile(false, IGNORE_STAMP, registry, null);
    assertFalse(disabled.hasRules("upstreamLineage"));
    assertEquals(disabled.walkCount(), 0);

    SemanticNoOpComparator ruled = compile(IGNORE_STAMP);
    assertFalse(ruled.hasRules("status"));
    assertEquals(ruled.walkCount(), 0);
    assertFalse(
        ruled.equivalent(
            "upstreamLineage",
            lineage(1L, "unknown", upstreamA).data(),
            lineage(1L, "other", upstreamB).data()));
    assertEquals(ruled.walkCount(), 1);
  }

  @Test
  public void ignoreTreatsTimestampAndPresenceAsNoOpButNotTheGraph() throws Exception {
    SemanticNoOpComparator comparator = compile(IGNORE_STAMP);
    UpstreamLineage stored = lineage(1L, "unknown", upstreamA);
    DataMap storedData = stored.data();
    DataMap storedCopy = storedData.copy();

    UpstreamLineage restamped = lineage(99L, "unknown", upstreamA);
    DataMap incoming = restamped.data();
    DataMap incomingCopy = incoming.copy();
    assertTrue(comparator.equivalent("upstreamLineage", storedData, incoming));
    assertEquals(storedData, storedCopy);
    assertEquals(incoming, incomingCopy);
    assertSame(storedData, stored.data());

    assertTrue(
        comparator.equivalent(
            "upstreamLineage", stored.data(), lineage(1L, "someone-else", upstreamA).data()));
    assertFalse(
        comparator.equivalent(
            "upstreamLineage", stored.data(), lineage(1L, "unknown", upstreamB).data()));
    assertFalse(
        comparator.equivalent(
            "upstreamLineage",
            stored.data(),
            lineage(1L, "unknown", upstreamA, DatasetLineageType.COPY).data()));

    UpstreamLineage reordered = new UpstreamLineage();
    UpstreamArray swapped = new UpstreamArray();
    swapped.add(upstream(upstreamB, 1L, "unknown"));
    swapped.add(upstream(upstreamA, 1L, "unknown"));
    reordered.setUpstreams(swapped);
    UpstreamLineage ordered = new UpstreamLineage();
    UpstreamArray both = new UpstreamArray();
    both.add(upstream(upstreamA, 1L, "unknown"));
    both.add(upstream(upstreamB, 1L, "unknown"));
    ordered.setUpstreams(both);
    assertFalse(comparator.equivalent("upstreamLineage", ordered.data(), reordered.data()));

    UpstreamLineage added = new UpstreamLineage();
    UpstreamArray extra = new UpstreamArray();
    extra.add(upstream(upstreamA, 1L, "unknown"));
    extra.add(upstream(upstreamB, 1L, "unknown"));
    added.setUpstreams(extra);
    assertFalse(comparator.equivalent("upstreamLineage", stored.data(), added.data()));

    UpstreamLineage withEdge = lineage(1L, "unknown", upstreamA);
    withEdge.setFineGrainedLineages(new FineGrainedLineageArray(edge("c1")));
    UpstreamLineage otherEdge = lineage(1L, "unknown", upstreamA);
    otherEdge.setFineGrainedLineages(new FineGrainedLineageArray(edge("c2")));
    assertFalse(comparator.equivalent("upstreamLineage", withEdge.data(), otherEdge.data()));
  }

  @Test
  public void actorOutsideAnIgnoredTimePathIsARealChange() {
    SemanticNoOpComparator comparator = compile(IGNORE_TIME);
    assertTrue(
        comparator.equivalent(
            "upstreamLineage",
            lineage(1L, "unknown", upstreamA).data(),
            lineage(50L, "unknown", upstreamA).data()));
    assertFalse(
        comparator.equivalent(
            "upstreamLineage",
            lineage(1L, "unknown", upstreamA).data(),
            lineage(1L, "real-actor", upstreamA).data()));
  }

  @Test
  public void timestampWindowCoalescesAgainstTheComparedValue() {
    SemanticNoOpComparator comparator = compile(WINDOW_TIME);
    DataMap stored = lineage(1_000L, "unknown", upstreamA).data();
    assertTrue(
        comparator.equivalent(
            "upstreamLineage", stored, lineage(2_000L, "unknown", upstreamA).data()));
    assertTrue(
        comparator.equivalent(
            "upstreamLineage", stored, lineage(1_000L, "unknown", upstreamA).data()));
    assertFalse(
        comparator.equivalent(
            "upstreamLineage", stored, lineage(2_001L, "unknown", upstreamA).data()));
    // Drift is against the value passed in, which stays the last persisted body.
    assertFalse(
        comparator.equivalent(
            "upstreamLineage", stored, lineage(3_000L, "unknown", upstreamA).data()));
    assertFalse(
        comparator.equivalent(
            "upstreamLineage",
            stored,
            withoutAuditTime(lineage(1_000L, "unknown", upstreamA)).data()));

    assertFalse(
        SemanticNoOpComparator.withinWindow(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
    assertTrue(SemanticNoOpComparator.withinWindow(Long.MAX_VALUE - 5, Long.MAX_VALUE, 5));
    assertFalse(SemanticNoOpComparator.withinWindow(Long.MIN_VALUE, Long.MIN_VALUE + 1, 0));
  }

  @Test
  public void assertionDefinitionIgnoresCreatedButNotTheDefinition() throws Exception {
    SemanticNoOpComparator comparator = compile(IGNORE_CREATED);
    AssertionInfo stored = assertion(10L, "id");
    AssertionInfo restamped = assertion(99L, "id");
    assertTrue(comparator.equivalent("assertionInfo", stored.data(), restamped.data()));

    AssertionInfo missingCreated = assertion(10L, "id");
    missingCreated.getSource().removeCreated();
    assertTrue(comparator.equivalent("assertionInfo", stored.data(), missingCreated.data()));

    assertFalse(
        comparator.equivalent("assertionInfo", stored.data(), assertion(10L, "other").data()));
    AssertionInfo otherType = assertion(10L, "id");
    otherType.setType(AssertionType.CUSTOM);
    assertFalse(comparator.equivalent("assertionInfo", stored.data(), otherType.data()));
    AssertionInfo otherEntity = assertion(10L, "id");
    otherEntity.setEntityUrn(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.other,PROD)"));
    assertFalse(comparator.equivalent("assertionInfo", stored.data(), otherEntity.data()));
    AssertionInfo otherField = assertion(10L, "id");
    otherField
        .getDatasetAssertion()
        .setFields(
            new UrnArray(
                UrnUtils.getUrn(
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.events,PROD),other)")));
    assertFalse(comparator.equivalent("assertionInfo", stored.data(), otherField.data()));
  }

  @Test
  public void explicitIgnoreIsWhatSuppressesARealEventTime() {
    DataMap stored = lineage(1_700_000_000_000L, "unknown", upstreamA).data();
    DataMap later = lineage(1_700_000_001_001L, "unknown", upstreamA).data();
    assertFalse(SemanticNoOpComparator.disabled().hasRules("upstreamLineage"));
    assertFalse(stored.equals(later));
    assertTrue(compile(IGNORE_STAMP).equivalent("upstreamLineage", stored, later));
    assertFalse(compile(WINDOW_TIME).equivalent("upstreamLineage", stored, later));
  }

  @Test
  public void suppressedWriteIncrementsOnlyThePreregisteredCounter() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricUtils metrics = MetricUtils.builder().registry(registry).build();
    SemanticNoOpComparator comparator =
        SemanticNoOpComparator.compile(true, IGNORE_STAMP, this.registry, metrics);
    assertEquals(registry.get(SemanticNoOpComparator.SUPPRESSED_COUNTER).counter().count(), 0d);
    assertTrue(
        comparator.equivalent(
            "upstreamLineage",
            lineage(1L, "unknown", upstreamA).data(),
            lineage(2L, "unknown", upstreamA).data()));
    assertEquals(registry.get(SemanticNoOpComparator.SUPPRESSED_COUNTER).counter().count(), 1d);
    assertFalse(
        comparator.equivalent(
            "upstreamLineage",
            lineage(1L, "unknown", upstreamA).data(),
            lineage(2L, "unknown", upstreamB).data()));
    assertEquals(registry.get(SemanticNoOpComparator.SUPPRESSED_COUNTER).counter().count(), 1d);
  }

  @Test
  public void startupRejectsInvalidRules() {
    expectThrows(
        IllegalArgumentException.class,
        () -> SemanticNoOpComparator.compile(false, "{", registry, null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp\",\"strategy\":\"NOPE\"}]",
                registry,
                null));
    String stampRule =
        "{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp\",\"strategy\":\"IGNORE\"}";
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true, "[" + stampRule + "," + stampRule + "]", registry, null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                false,
                "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp\",\"strategy\":\"IGNORE\"},"
                    + "{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp/time\",\"strategy\":\"IGNORE\"}]",
                registry,
                null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/**/auditStamp\",\"strategy\":\"IGNORE\"}]",
                registry,
                null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"notAnAspect\",\"path\":\"/source\",\"strategy\":\"IGNORE\"}]",
                registry,
                null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"upstreamLineage\",\"path\":\"/missing/*/auditStamp\",\"strategy\":\"IGNORE\"}]",
                registry,
                null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"assertionInfo\",\"path\":\"/source/type\",\"strategy\":\"TIMESTAMP_WINDOW\",\"maxDeltaMs\":1}]",
                registry,
                null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp/time\",\"strategy\":\"TIMESTAMP_WINDOW\",\"maxDeltaMs\":-1}]",
                registry,
                null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            SemanticNoOpComparator.compile(
                true,
                "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp\",\"strategy\":\"IGNORE\",\"maxDeltaMs\":1}]",
                registry,
                null));
  }

  private SemanticNoOpComparator compile(String rulesJson) {
    return SemanticNoOpComparator.compile(true, rulesJson, registry, null);
  }

  private UpstreamLineage lineage(long time, String actor, Urn dataset) {
    return lineage(time, actor, dataset, DatasetLineageType.TRANSFORMED);
  }

  private UpstreamLineage lineage(long time, String actor, Urn dataset, DatasetLineageType type) {
    UpstreamLineage lineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    Upstream upstream = upstream(dataset, time, actor);
    upstream.setType(type);
    upstreams.add(upstream);
    lineage.setUpstreams(upstreams);
    return lineage;
  }

  private Upstream upstream(Urn dataset, long time, String actor) {
    try {
      return new Upstream()
          .setDataset(DatasetUrn.createFromUrn(dataset))
          .setType(DatasetLineageType.TRANSFORMED)
          .setAuditStamp(new AuditStamp().setTime(time).setActor(new CorpuserUrn(actor)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private UpstreamLineage withoutAuditTime(UpstreamLineage lineage) {
    UpstreamLineage copy;
    try {
      copy = lineage.copy();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    ((DataMap) copy.data().getDataList("upstreams").get(0)).remove("auditStamp");
    return copy;
  }

  private FineGrainedLineage edge(String column) {
    Urn field =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.a,PROD),"
                + column
                + ")");
    return new FineGrainedLineage()
        .setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET)
        .setUpstreams(new UrnArray(field))
        .setDownstreamType(FineGrainedLineageDownstreamType.FIELD)
        .setDownstreams(new UrnArray(field));
  }

  private AssertionInfo assertion(long created, String parameter) throws Exception {
    StringMap nativeParameters = new StringMap();
    nativeParameters.put("column", parameter);
    return new AssertionInfo()
        .setType(AssertionType.DATASET)
        .setEntityUrn(asserted)
        .setSource(
            new AssertionSource()
                .setType(AssertionSourceType.EXTERNAL)
                .setCreated(new AuditStamp().setTime(created).setActor(new CorpuserUrn("unknown"))))
        .setDatasetAssertion(
            new DatasetAssertionInfo()
                .setDataset(DatasetUrn.createFromUrn(asserted))
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                .setOperator(AssertionStdOperator.EQUAL_TO)
                .setFields(
                    new UrnArray(
                        UrnUtils.getUrn(
                            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.events,PROD),id)")))
                .setNativeParameters(nativeParameters));
  }
}
