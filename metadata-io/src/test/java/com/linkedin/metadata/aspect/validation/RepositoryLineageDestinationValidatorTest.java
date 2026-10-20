package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.REPOSITORY_LINEAGE_ASPECT_NAME;
import static org.testng.Assert.assertEquals;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.repository.RepositoryLineage;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RepositoryLineageDestinationValidatorTest {

  private static final Urn DATA_JOB =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,my_flow,PROD),my_task)");
  private static final Urn DATA_FLOW = UrnUtils.getUrn("urn:li:dataFlow:(airflow,my_flow,PROD)");
  private static final Urn REPOSITORY = UrnUtils.getUrn("urn:li:repository:github.acme/repo1");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  private final RepositoryLineageDestinationValidator validator =
      new RepositoryLineageDestinationValidator()
          .setConfig(
              AspectPluginConfig.builder()
                  .className("test")
                  .enabled(true)
                  .supportedOperations(List.of("UPSERT", "PATCH"))
                  .supportedEntityAspectNames(
                      List.of(
                          AspectPluginConfig.EntityAspectName.builder()
                              .entityName("*")
                              .aspectName(REPOSITORY_LINEAGE_ASPECT_NAME)
                              .build()))
                  .build());

  private long violations(Urn entity, List<Urn> inputs, List<Urn> outputs) {
    RepositoryLineage lineage = new RepositoryLineage();
    lineage.setInputEdges(edges(inputs));
    lineage.setOutputEdges(edges(outputs));
    ChangeMCP item =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(entity)
            .entitySpec(registry.getEntitySpec(entity.getEntityType()))
            .aspectSpec(
                registry
                    .getEntitySpec(entity.getEntityType())
                    .getAspectSpec(REPOSITORY_LINEAGE_ASPECT_NAME))
            .recordTemplate(lineage)
            .build();
    return validator
        .validatePreCommit(
            OperationFingerprint.EMPTY, List.of(item), Mockito.mock(RetrieverContext.class))
        .count();
  }

  private static EdgeArray edges(List<Urn> destinations) {
    EdgeArray edges = new EdgeArray();
    destinations.forEach(d -> edges.add(new Edge().setDestinationUrn(d)));
    return edges;
  }

  @Test
  public void testRepositoryDestinationAllowedOnPipelinesAndTasks() {
    assertEquals(violations(DATA_JOB, List.of(REPOSITORY), List.of()), 0);
    assertEquals(violations(DATA_FLOW, List.of(REPOSITORY), List.of()), 0);
  }

  @Test
  public void testDatasetDestinationRejectedInBothDirections() {
    assertEquals(violations(DATA_JOB, List.of(REPOSITORY, DATASET), List.of()), 1);
    assertEquals(violations(DATA_FLOW, List.of(), List.of(DATASET)), 1);
  }
}
