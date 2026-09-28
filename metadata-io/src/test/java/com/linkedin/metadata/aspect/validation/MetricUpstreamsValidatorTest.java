package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.METRIC_UPSTREAMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.UPSTREAM_METRICS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UpstreamMetrics;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metric.MetricUpstreams;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetricUpstreamsValidatorTest {

  private static final Urn METRIC_URN =
      UrnUtils.getUrn("urn:li:metric:(urn:li:dataPlatform:snowflake,db.sch.sales,revenue)");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.orders,PROD)");
  private static final Urn OTHER_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.customers,PROD)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
  private final ObjectMapper objectMapper =
      TestOperationContexts.systemContextNoSearchAuthorization().getObjectMapper();
  private final AuditStamp auditStamp =
      new AuditStamp().setTime(1000L).setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));

  private MetricUpstreamsValidator validator;
  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private final Map<Urn, Map<String, Aspect>> currentAspects = new HashMap<>();

  @BeforeMethod
  public void setup() {
    currentAspects.clear();
    validator =
        new MetricUpstreamsValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "PATCH"))
                    .supportedEntityAspectNames(List.of())
                    .build());
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(registry);
    Mockito.doAnswer(
            invocation -> {
              Set<Urn> requestedUrns = invocation.getArgument(1);
              Set<String> requestedAspects = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              requestedUrns.forEach(
                  urn -> {
                    Map<String, Aspect> byName = new HashMap<>();
                    currentAspects
                        .getOrDefault(urn, Map.of())
                        .forEach(
                            (aspectName, aspect) -> {
                              if (requestedAspects.contains(aspectName)) {
                                byName.put(aspectName, aspect);
                              }
                            });
                    if (!byName.isEmpty()) {
                      result.put(urn, byName);
                    }
                  });
              return result;
            })
        .when(mockAspectRetriever)
        .getLatestAspectObjects(any(OperationFingerprint.class), anySet(), anySet());

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  @Test
  public void testAcceptsEmptyFieldUpstreams() {
    MetricUpstreams aspect =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray());

    Assert.assertTrue(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testAcceptsDatasetOnly() {
    MetricUpstreams aspect =
        new MetricUpstreams().setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)));

    Assert.assertTrue(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testRejectsWhenDatasetAlreadyConsumesMetric() {
    UpstreamMetrics consumer =
        new UpstreamMetrics().setMetrics(new EdgeArray(datasetEdge(METRIC_URN)));
    currentAspects
        .computeIfAbsent(DATASET_URN, u -> new HashMap<>())
        .put(UPSTREAM_METRICS_ASPECT_NAME, new Aspect(consumer.data()));

    MetricUpstreams aspect =
        new MetricUpstreams().setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)));

    Assert.assertFalse(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testAcceptsWhenDatasetDoesNotConsumeMetric() {
    UpstreamMetrics consumer = new UpstreamMetrics().setMetrics(new EdgeArray());
    currentAspects
        .computeIfAbsent(DATASET_URN, u -> new HashMap<>())
        .put(UPSTREAM_METRICS_ASPECT_NAME, new Aspect(consumer.data()));

    MetricUpstreams aspect =
        new MetricUpstreams().setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)));

    Assert.assertTrue(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testAcceptsMatchingColumnParents() {
    MetricUpstreams aspect =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray(fieldEdge(DATASET_URN, "AMOUNT")));

    Assert.assertTrue(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testRejectsMissingColumnParent() {
    MetricUpstreams aspect =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray(fieldEdge(OTHER_DATASET_URN, "AMOUNT")));

    Assert.assertFalse(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testRejectsColumnsWithoutDatasetUpstreams() {
    MetricUpstreams aspect =
        new MetricUpstreams().setFieldUpstreams(new EdgeArray(fieldEdge(DATASET_URN, "AMOUNT")));

    Assert.assertFalse(validateUpsert(aspect).isEmpty());
  }

  @Test
  public void testPatchFieldOnlyAcceptsWhenParentAlreadyStored() {
    MetricUpstreams current =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray());
    stubCurrent(current);

    Assert.assertTrue(validateProposedItemPatch(fieldOnlyReplaceOp()).isEmpty());
  }

  @Test
  public void testPatchItemImplFieldOnlyAcceptsWhenParentAlreadyStored() {
    MetricUpstreams current =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray());
    stubCurrent(current);

    Assert.assertTrue(validatePatchItemImpl(fieldOnlyReplaceOp()).isEmpty());
  }

  @Test
  public void testPatchDatasetOnlyRejectsWhenDroppingParentOfStoredFields() {
    MetricUpstreams current =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray(fieldEdge(DATASET_URN, "AMOUNT")));
    stubCurrent(current);

    Assert.assertFalse(validateProposedItemPatch(datasetOnlyReplaceOp()).isEmpty());
  }

  @Test
  public void testPatchItemImplDatasetOnlyRejectsWhenDroppingParentOfStoredFields() {
    MetricUpstreams current =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray(fieldEdge(DATASET_URN, "AMOUNT")));
    stubCurrent(current);

    Assert.assertFalse(validatePatchItemImpl(datasetOnlyReplaceOp()).isEmpty());
  }

  private List<AspectValidationException> validateUpsert(MetricUpstreams aspect) {
    BatchItem item =
        TestMCP.ofOneUpsertItem(METRIC_URN, aspect, registry).stream().findFirst().get();
    return validator
        .validateProposedAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private List<AspectValidationException> validateProposedItemPatch(
      GenericJsonPatch.PatchOp patchOp) {
    ProposedItem proposedItem =
        ProposedItem.builder().build(buildPatchMcp(List.of(patchOp)), auditStamp, registry);
    return validator
        .validateProposedAspects(
            OperationFingerprint.EMPTY, List.of(proposedItem), retrieverContext)
        .toList();
  }

  private List<AspectValidationException> validatePatchItemImpl(GenericJsonPatch.PatchOp patchOp) {
    PatchItemImpl patchItem =
        PatchItemImpl.builder().build(buildPatchMcp(List.of(patchOp)), auditStamp, registry);
    return validator
        .validateProposedAspects(OperationFingerprint.EMPTY, List.of(patchItem), retrieverContext)
        .toList();
  }

  private MetadataChangeProposal buildPatchMcp(List<GenericJsonPatch.PatchOp> ops) {
    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder().patch(ops).forceGenericPatch(true).build();

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(METRIC_URN);
    mcp.setEntityType(METRIC_URN.getEntityType());
    mcp.setAspectName(METRIC_UPSTREAMS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.PATCH);
    mcp.setAspect(GenericRecordUtils.serializePatch(genericJsonPatch, objectMapper));
    return mcp;
  }

  private GenericJsonPatch.PatchOp fieldOnlyReplaceOp() {
    Urn fieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(DATASET_URN, "AMOUNT");
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("replace");
    patchOp.setPath("/fieldUpstreams");
    patchOp.setValue(
        objectMapper.convertValue(
            List.of(Map.of("destinationUrn", fieldUrn.toString())), JsonNode.class));
    return patchOp;
  }

  private GenericJsonPatch.PatchOp datasetOnlyReplaceOp() {
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("replace");
    patchOp.setPath("/datasetUpstreams");
    patchOp.setValue(
        objectMapper.convertValue(
            List.of(Map.of("destinationUrn", OTHER_DATASET_URN.toString())), JsonNode.class));
    return patchOp;
  }

  private void stubCurrent(MetricUpstreams aspect) {
    currentAspects
        .computeIfAbsent(METRIC_URN, u -> new HashMap<>())
        .put(METRIC_UPSTREAMS_ASPECT_NAME, new Aspect(aspect.data()));
  }

  private static Edge datasetEdge(Urn datasetUrn) {
    return new Edge().setDestinationUrn(datasetUrn);
  }

  private static Edge fieldEdge(Urn datasetUrn, String fieldPath) {
    return new Edge()
        .setDestinationUrn(SchemaFieldUtils.generateSchemaFieldUrn(datasetUrn, fieldPath));
  }
}
