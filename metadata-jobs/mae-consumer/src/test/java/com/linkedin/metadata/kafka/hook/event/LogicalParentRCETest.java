package com.linkedin.metadata.kafka.hook.event;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.RelationshipChangeEvent;
import com.linkedin.platform.event.v1.RelationshipChangeOperation;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

/**
 * Regression tests for PhysicalInstanceOf RelationshipChangeEvent emission.
 *
 * <p>Background: When a physical dataset is linked to a logical dataset via the logicalParent
 * aspect, a PhysicalInstanceOf RelationshipChangeEvent (RCE) should be emitted by
 * PlatformEventGeneratorHook. The integrations service depends on this RCE to trigger SP
 * propagation in the parent-then-child ingestion order (logical SP set before the physical is
 * linked).
 *
 * <p>Two ingestion orderings are tested:
 *
 * <ul>
 *   <li>child-then-parent: physical linked first (logicalParent set), then SP set on logical.
 *       Propagation works via the ECE path — SP ECE fires against an already-indexed edge.
 *   <li>parent-then-child: SP set on logical first, then physical linked (logicalParent set).
 *       Propagation must work via the RCE path — the new edge RCE must trigger SP propagation.
 * </ul>
 *
 * <p>These unit tests assert that PlatformEventGeneratorHook correctly emits a PhysicalInstanceOf
 * RCE when a logicalParent MCL is processed, which is the prerequisite for the parent-then-child
 * path to work.
 *
 * <p>NOTE: These tests use the real entity registry (loaded from entity-registry.yml) rather than
 * the mocked registry used in PlatformEventGeneratorHookTest. The mocked registry does not register
 * logicalParent, so getAspectSpec("logicalParent") returns null there, causing
 * buildRelationshipChangeEvents to short-circuit early with an empty result. The real registry
 * contains the @Relationship annotation on LogicalParent.pdl and is required for these tests to
 * exercise the actual code path.
 */
public class LogicalParentRCETest {

  private static final long EVENT_TIME = 123L;

  private static final String PHYSICAL_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,physical_dataset,PROD)";
  private static final String LOGICAL_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,logical_dataset,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private EventProducer mockProducer;
  private PlatformEventGeneratorHook hook;
  private Urn actorUrn;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    mockProducer = Mockito.mock(EventProducer.class);

    OperationContext realOpContext = TestOperationContexts.systemContextNoSearchAuthorization();

    hook =
        new PlatformEventGeneratorHook(
            realOpContext, new EntityChangeEventGeneratorRegistry(), mockProducer, true);
  }

  /**
   * Asserts that processing a logicalParent MCL (physical linked to logical for the first time)
   * emits a PhysicalInstanceOf ADD RelationshipChangeEvent on the PlatformEvent_v1 topic.
   *
   * <p>This is the core prerequisite for the parent-then-child propagation path. If no RCE is
   * emitted here, the integrations service never learns about the new physical-to-logical
   * relationship and cannot propagate SPs that were set on the logical before the link existed.
   */
  @Test
  public void testLogicalParentMCLEmitsPhysicalInstanceOfAddRCE() throws Exception {
    Urn physicalUrn = UrnUtils.getUrn(PHYSICAL_DATASET_URN);
    Urn logicalUrn = UrnUtils.getUrn(LOGICAL_DATASET_URN);

    // Build the logicalParent aspect: physical -> logical
    LogicalParent logicalParent = new LogicalParent();
    Edge parentEdge = new Edge();
    parentEdge.setDestinationUrn(logicalUrn);
    logicalParent.setParent(parentEdge);

    // MCL: new logicalParent on the physical dataset (no previous value — first-time link)
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityType(DATASET_ENTITY_NAME);
    mcl.setEntityUrn(physicalUrn);
    mcl.setAspectName(LOGICAL_PARENT_ASPECT_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    mcl.setAspect(GenericRecordUtils.serializeAspect(logicalParent));
    mcl.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    hook.invoke(mcl);

    // Verify a RelationshipChangeEvent was produced with the expected fields.
    verify(mockProducer, times(1))
        .producePlatformEvent(
            eq(RELATIONSHIP_PLATFORM_EVENT_NAME),
            anyString(),
            argThat(
                (PlatformEvent event) -> {
                  if (!RELATIONSHIP_PLATFORM_EVENT_NAME.equals(event.getName())) return false;
                  RelationshipChangeEvent rce =
                      GenericRecordUtils.deserializePayload(
                          event.getPayload().getValue(), RelationshipChangeEvent.class);
                  return "PhysicalInstanceOf".equals(rce.getRelationshipType())
                      && RelationshipChangeOperation.ADD.equals(rce.getOperation())
                      && physicalUrn.equals(rce.getSourceUrn())
                      && logicalUrn.equals(rce.getDestinationUrn());
                }));
  }

  /**
   * Asserts that removing a logicalParent link emits a PhysicalInstanceOf REMOVE
   * RelationshipChangeEvent.
   *
   * <p>Companion to the ADD test — ensures both sides of the edge lifecycle emit RCEs.
   */
  @Test
  public void testLogicalParentRemovedEmitsPhysicalInstanceOfRemoveRCE() throws Exception {
    Urn physicalUrn = UrnUtils.getUrn(PHYSICAL_DATASET_URN);
    Urn logicalUrn = UrnUtils.getUrn(LOGICAL_DATASET_URN);

    // Previous aspect had a parent edge; new aspect has no parent (link removed)
    LogicalParent previousLogicalParent = new LogicalParent();
    Edge parentEdge = new Edge();
    parentEdge.setDestinationUrn(logicalUrn);
    previousLogicalParent.setParent(parentEdge);

    // new aspect: parent field absent — link removed
    LogicalParent newLogicalParent = new LogicalParent();

    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityType(DATASET_ENTITY_NAME);
    mcl.setEntityUrn(physicalUrn);
    mcl.setAspectName(LOGICAL_PARENT_ASPECT_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    mcl.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousLogicalParent));
    mcl.setAspect(GenericRecordUtils.serializeAspect(newLogicalParent));
    mcl.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    hook.invoke(mcl);

    verify(mockProducer, times(1))
        .producePlatformEvent(
            eq(RELATIONSHIP_PLATFORM_EVENT_NAME),
            anyString(),
            argThat(
                (PlatformEvent event) -> {
                  if (!RELATIONSHIP_PLATFORM_EVENT_NAME.equals(event.getName())) return false;
                  RelationshipChangeEvent rce =
                      GenericRecordUtils.deserializePayload(
                          event.getPayload().getValue(), RelationshipChangeEvent.class);
                  return "PhysicalInstanceOf".equals(rce.getRelationshipType())
                      && RelationshipChangeOperation.REMOVE.equals(rce.getOperation())
                      && physicalUrn.equals(rce.getSourceUrn())
                      && logicalUrn.equals(rce.getDestinationUrn());
                }));
  }

  /**
   * Contract test: asserts the computed relationship-change supported aspect names exactly match
   * the hardcoded expected set. If this test fails, a PDL-defined lineage relationship type was
   * added or removed — update the expected set and confirm the change is intentional.
   */
  @Test
  public void testRelationshipChangeSupportedAspectNamesMatchExpectedSet() {
    Set<String> expected =
        Sets.newHashSet(
            // All discovered via isLineage=true relationship annotations in PDL files
            UPSTREAM_LINEAGE_ASPECT_NAME,
            DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
            "dataProcessInfo",
            "dataProcessInstanceInput",
            "dataProcessInstanceOutput",
            "chartInfo",
            "dashboardInfo",
            "mlModelProperties",
            "mlModelGroupProperties",
            "mlFeatureProperties",
            "mlPrimaryKeyProperties",
            // Explicit: PhysicalInstanceOf lacks isLineage=true but must trigger RCEs
            LOGICAL_PARENT_ASPECT_NAME);

    Assert.assertEquals(
        hook.getRelationshipChangeSupportedAspectNames(),
        expected,
        "Lineage aspect set changed — update this list and verify the change is intentional");
  }
}
