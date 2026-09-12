package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metric.MetricUpstreams;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
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

  private MetricUpstreamsValidator validator;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    validator =
        new MetricUpstreamsValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(Mockito.mock(CachingAspectRetriever.class))
            .build();
  }

  @Test
  public void testAcceptsEmptyFieldUpstreams() {
    MetricUpstreams aspect =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray());

    Assert.assertTrue(validate(aspect).isEmpty());
  }

  @Test
  public void testAcceptsDatasetOnly() {
    MetricUpstreams aspect =
        new MetricUpstreams().setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)));

    Assert.assertTrue(validate(aspect).isEmpty());
  }

  @Test
  public void testAcceptsMatchingColumnParents() {
    MetricUpstreams aspect =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray(fieldEdge(DATASET_URN, "AMOUNT")));

    Assert.assertTrue(validate(aspect).isEmpty());
  }

  @Test
  public void testRejectsMissingColumnParent() {
    MetricUpstreams aspect =
        new MetricUpstreams()
            .setDatasetUpstreams(new EdgeArray(datasetEdge(DATASET_URN)))
            .setFieldUpstreams(new EdgeArray(fieldEdge(OTHER_DATASET_URN, "AMOUNT")));

    Assert.assertFalse(validate(aspect).isEmpty());
  }

  @Test
  public void testRejectsColumnsWithoutDatasetUpstreams() {
    MetricUpstreams aspect =
        new MetricUpstreams().setFieldUpstreams(new EdgeArray(fieldEdge(DATASET_URN, "AMOUNT")));

    Assert.assertFalse(validate(aspect).isEmpty());
  }

  private List<AspectValidationException> validate(MetricUpstreams aspect) {
    BatchItem item =
        TestMCP.ofOneUpsertItem(METRIC_URN, aspect, registry).stream().findFirst().get();
    return validator
        .validateProposedAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private static Edge datasetEdge(Urn datasetUrn) {
    return new Edge().setDestinationUrn(datasetUrn);
  }

  private static Edge fieldEdge(Urn datasetUrn, String fieldPath) {
    return new Edge()
        .setDestinationUrn(SchemaFieldUtils.generateSchemaFieldUrn(datasetUrn, fieldPath));
  }
}
