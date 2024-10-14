package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DocumentationServiceTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:admin");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_DASHBOARD_URN = UrnUtils.getUrn("urn:li:dashboard:(test,test)");
  private static final Urn TEST_CHART_URN = UrnUtils.getUrn("urn:li:chart:(test,test)");
  private static final Urn TEST_DATA_FLOW_URN = UrnUtils.getUrn("urn:li:dataFlow:(test,test,test)");
  private static final Urn TEST_DATA_JOB_URN =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(test,test,test),test)");
  private static final Urn TEST_TERM_URN = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_TERM_GROUP_URN = UrnUtils.getUrn("urn:li:glossaryNode:test");
  private static final Urn TEST_CONTAINER_URN = UrnUtils.getUrn("urn:li:container:test");
  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:test");
  private static final Urn TEST_ML_MODEL_URN =
      UrnUtils.getUrn("urn:li:mlModel:(urn:li:dataPlatform:sagemaker,cypress-model,PROD)");
  private static final Urn TEST_ML_MODEL_GROUP_URN =
      UrnUtils.getUrn(
          "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,cypress-model-package-group,PROD)");
  private static final Urn TEST_ML_FEATURE_TABLE_URN =
      UrnUtils.getUrn(
          "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,test_feature_table_all_feature_dtypes)");
  private static final Urn TEST_ML_FEATURE_URN =
      UrnUtils.getUrn("urn:li:mlFeature:(cypress-test-2,some-cypress-feature-1)");
  private static final Urn TEST_ML_PRIMARY_KEY_URN =
      UrnUtils.getUrn("urn:li:mlPrimaryKey:(cypress-test-2,some-cypress-feature-2)");
  private static final Urn TEST_DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:test");
  private static final Urn TEST_ENTITY_TYPE_URN = UrnUtils.getUrn("urn:li:entityType:test123");
  private static final String TEST_DOCUMENTATION = "Test documentation";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private OperationContext opContext;
  private SystemEntityClient mockClient;

  @BeforeTest
  public void setup() {
    mockClient = mock(SystemEntityClient.class);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @Test
  public void testUpdateDatasetDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_DATASET_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(mockClient, TEST_DATASET_URN, DATASET_ENTITY_NAME, DATASET_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateDashboardDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_DASHBOARD_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_DASHBOARD_URN,
        DASHBOARD_ENTITY_NAME,
        EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateChartDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_CHART_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient, TEST_CHART_URN, CHART_ENTITY_NAME, EDITABLE_CHART_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateDataFlowDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_DATA_FLOW_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_DATA_FLOW_URN,
        DATA_FLOW_ENTITY_NAME,
        EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateDataJobDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_DATA_JOB_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_DATA_JOB_URN,
        DATA_JOB_ENTITY_NAME,
        EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateGlossaryTermDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_TERM_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient, TEST_TERM_URN, GLOSSARY_TERM_ENTITY_NAME, GLOSSARY_TERM_INFO_ASPECT_NAME);
  }

  @Test
  public void testUpdateGlossaryNodeDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_TERM_GROUP_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient, TEST_TERM_GROUP_URN, GLOSSARY_NODE_ENTITY_NAME, GLOSSARY_NODE_INFO_ASPECT_NAME);
  }

  @Test
  public void testUpdateContainerDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_CONTAINER_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_CONTAINER_URN,
        CONTAINER_ENTITY_NAME,
        CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateDomainDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_DOMAIN_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(mockClient, TEST_DOMAIN_URN, DOMAIN_ENTITY_NAME, DOMAIN_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateMLModelDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_ML_MODEL_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_ML_MODEL_URN,
        ML_MODEL_ENTITY_NAME,
        ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateMLModelGroupDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(
        opContext, TEST_ML_MODEL_GROUP_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_ML_MODEL_GROUP_URN,
        ML_MODEL_GROUP_ENTITY_NAME,
        ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateMLFeatureTableDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(
        opContext, TEST_ML_FEATURE_TABLE_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_ML_FEATURE_TABLE_URN,
        ML_FEATURE_TABLE_ENTITY_NAME,
        ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateMLFeatureDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(opContext, TEST_ML_FEATURE_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_ML_FEATURE_URN,
        ML_FEATURE_ENTITY_NAME,
        ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateMLPrimaryKeyDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(
        opContext, TEST_ML_PRIMARY_KEY_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_ML_PRIMARY_KEY_URN,
        ML_PRIMARY_KEY_ENTITY_NAME,
        ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testUpdateDataProductDocumentation() throws Exception {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    service.updateDocumentation(
        opContext, TEST_DATA_PRODUCT_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);

    // Verify that ingestProposal was called once
    verifyIngest(
        mockClient,
        TEST_DATA_PRODUCT_URN,
        DATA_PRODUCT_ENTITY_NAME,
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testFailUnsupportedEntityType() {
    DocumentationService service =
        new DocumentationService(
            mockClient, mock(OpenApiClient.class), objectMapper, METADATA_TESTS_SOURCE, true);

    // throws with unsupported entity type
    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          service.updateDocumentation(
              opContext, TEST_ENTITY_TYPE_URN, TEST_DOCUMENTATION, TEST_ACTOR_URN);
        });
  }

  private static MetadataChangeProposal createPatchMcp(
      Urn urn, String entityType, String aspectName) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(entityType);
    mcp.setAspectName(aspectName);
    mcp.setChangeType(ChangeType.PATCH);
    return mcp;
  }

  private static void verifyIngest(
      EntityClient mockClient, Urn urn, String entityType, String aspectName) throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new MetadataChangeProposalMatcher(
                    Collections.singletonList(createPatchMcp(urn, entityType, aspectName)))),
            Mockito.eq(true));
  }

  // it's very difficult to check the whole contents of the MCP, so check that we're producing the
  // right type of MCP
  private static class MetadataChangeProposalMatcher
      implements ArgumentMatcher<List<MetadataChangeProposal>> {

    private final List<MetadataChangeProposal> leftList;

    public MetadataChangeProposalMatcher(List<MetadataChangeProposal> leftList) {
      this.leftList = leftList;
    }

    @Override
    public boolean matches(List<MetadataChangeProposal> rightList) {
      return leftList.stream()
          .allMatch(
              left ->
                  rightList.stream()
                      .anyMatch(
                          right ->
                              left.getEntityType().equals(right.getEntityType())
                                  && left.getAspectName().equals(right.getAspectName())
                                  && left.getChangeType().equals(right.getChangeType())
                                  && left.getEntityUrn().equals(right.getEntityUrn())));
    }
  }
}
