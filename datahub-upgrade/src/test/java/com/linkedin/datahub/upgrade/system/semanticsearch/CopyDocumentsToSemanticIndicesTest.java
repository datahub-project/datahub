package com.linkedin.datahub.upgrade.system.semanticsearch;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CopyDocumentsToSemanticIndicesTest {

  @Mock private SearchClientShim<?> searchClient;

  @Mock private EntityService<?> entityService;

  @Mock private SemanticSearchConfiguration semanticSearchConfiguration;

  @Mock private IndexConvention indexConvention;

  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @Test
  public void testSteps_WithEnabledEntities() {
    Set<String> enabledEntities = Set.of("dataset", "chart", "dashboard");
    when(semanticSearchConfiguration.getEnabledEntities()).thenReturn(enabledEntities);

    CopyDocumentsToSemanticIndices upgrade =
        new CopyDocumentsToSemanticIndices(
            opContext,
            searchClient,
            entityService,
            semanticSearchConfiguration,
            indexConvention,
            true);

    List<UpgradeStep> steps = upgrade.steps();

    assertEquals(steps.size(), 3);

    assertTrue(
        steps.stream().anyMatch(step -> step.id().equals("CopyDocumentsToSemanticIndex_dataset")));
    assertTrue(
        steps.stream().anyMatch(step -> step.id().equals("CopyDocumentsToSemanticIndex_chart")));
    assertTrue(
        steps.stream()
            .anyMatch(step -> step.id().equals("CopyDocumentsToSemanticIndex_dashboard")));
  }
}
