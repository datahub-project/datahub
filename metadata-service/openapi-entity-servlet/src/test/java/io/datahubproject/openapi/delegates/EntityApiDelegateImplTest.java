package io.datahubproject.openapi.delegates;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.*;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.config.OpenAPIEntityTestConfiguration;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.generated.BrowsePathEntry;
import io.datahubproject.openapi.generated.BrowsePathsV2;
import io.datahubproject.openapi.generated.BrowsePathsV2AspectRequestV2;
import io.datahubproject.openapi.generated.ChartEntityRequestV2;
import io.datahubproject.openapi.generated.ChartEntityResponseV2;
import io.datahubproject.openapi.generated.DatasetEntityRequestV2;
import io.datahubproject.openapi.generated.DatasetEntityResponseV2;
import io.datahubproject.openapi.generated.Deprecation;
import io.datahubproject.openapi.generated.DeprecationAspectRequestV2;
import io.datahubproject.openapi.generated.Domains;
import io.datahubproject.openapi.generated.DomainsAspectRequestV2;
import io.datahubproject.openapi.generated.GlobalTags;
import io.datahubproject.openapi.generated.GlobalTagsAspectRequestV2;
import io.datahubproject.openapi.generated.GlossaryTermAssociation;
import io.datahubproject.openapi.generated.GlossaryTerms;
import io.datahubproject.openapi.generated.GlossaryTermsAspectRequestV2;
import io.datahubproject.openapi.generated.Owner;
import io.datahubproject.openapi.generated.Ownership;
import io.datahubproject.openapi.generated.OwnershipAspectRequestV2;
import io.datahubproject.openapi.generated.OwnershipType;
import io.datahubproject.openapi.generated.ScrollChartEntityResponseV2;
import io.datahubproject.openapi.generated.ScrollDatasetEntityResponseV2;
import io.datahubproject.openapi.generated.Status;
import io.datahubproject.openapi.generated.StatusAspectRequestV2;
import io.datahubproject.openapi.generated.TagAssociation;
import io.datahubproject.openapi.generated.controller.ChartApiController;
import io.datahubproject.openapi.generated.controller.DatasetApiController;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.generated.controller"})
@Import({OpenAPIEntityTestConfiguration.class})
@AutoConfigureMockMvc
public class EntityApiDelegateImplTest extends AbstractTestNGSpringContextTests {
  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Autowired private ChartApiController chartApiController;
  @Autowired private DatasetApiController datasetApiController;
  @Autowired private EntityRegistry entityRegistry;
  @Autowired private MockMvc mockMvc;

  @Test
  public void initTest() {
    assertNotNull(chartApiController);
    assertNotNull(datasetApiController);

    assertTrue(
        entityRegistry
            .getEntitySpec("dataset")
            .getAspectSpecMap()
            .containsKey("customDataQualityRules"),
        "Failed to load custom model from custom registry");
  }

  @Test
  public void chartApiControllerTest() {
    final String testUrn = "urn:li:chart:(looker,baz1)";

    ChartEntityRequestV2 req = ChartEntityRequestV2.builder().urn(testUrn).build();
    ChartEntityResponseV2 resp = chartApiController.create(List.of(req)).getBody().get(0);
    assertEquals(resp.getUrn(), testUrn);

    resp = chartApiController.get(testUrn, false, List.of()).getBody();
    assertEquals(resp.getUrn(), testUrn);

    ResponseEntity<Void> deleteResp = chartApiController.delete(testUrn);
    assertEquals(deleteResp.getStatusCode(), HttpStatus.OK);

    ResponseEntity<Void> headResp = chartApiController.head(testUrn);
    assertEquals(headResp.getStatusCode(), HttpStatus.NOT_FOUND);

    ResponseEntity<ScrollChartEntityResponseV2> scrollResp =
        chartApiController.scroll(false, List.of(), 10, null, null, null, null);
    assertEquals(scrollResp.getStatusCode(), HttpStatus.OK);
    assertNotNull(scrollResp.getBody().getEntities());
  }

  @Test
  public void datasetApiControllerTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    DatasetEntityRequestV2 req = DatasetEntityRequestV2.builder().urn(testUrn).build();
    DatasetEntityResponseV2 resp = datasetApiController.create(List.of(req)).getBody().get(0);
    assertEquals(resp.getUrn(), testUrn);

    resp = datasetApiController.get(testUrn, false, List.of()).getBody();
    assertEquals(resp.getUrn(), testUrn);

    ResponseEntity<Void> deleteResp = datasetApiController.delete(testUrn);
    assertEquals(deleteResp.getStatusCode(), HttpStatus.OK);

    ResponseEntity<Void> headResp = datasetApiController.head(testUrn);
    assertEquals(headResp.getStatusCode(), HttpStatus.NOT_FOUND);

    ResponseEntity<ScrollDatasetEntityResponseV2> scrollResp =
        datasetApiController.scroll(false, List.of(), 10, null, null, null, null);
    assertEquals(scrollResp.getStatusCode(), HttpStatus.OK);
    assertNotNull(scrollResp.getBody().getEntities());
  }

  @Test
  public void browsePathsTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    BrowsePathsV2AspectRequestV2 req =
        BrowsePathsV2AspectRequestV2.builder()
            .value(
                BrowsePathsV2.builder()
                    .path(List.of(BrowsePathEntry.builder().urn(testUrn).id("path").build()))
                    .build())
            .build();
    assertEquals(
        datasetApiController.createBrowsePathsV2(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteBrowsePathsV2(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getBrowsePathsV2(testUrn, false).getStatusCode(),
        HttpStatus.NOT_FOUND);
    assertEquals(
        datasetApiController.headBrowsePathsV2(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void deprecationTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    DeprecationAspectRequestV2 req =
        DeprecationAspectRequestV2.builder()
            .value(Deprecation.builder().deprecated(true).build())
            .build();
    assertEquals(
        datasetApiController.createDeprecation(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteDeprecation(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getDeprecation(testUrn, false).getStatusCode(), HttpStatus.NOT_FOUND);
    assertEquals(
        datasetApiController.headDeprecation(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void domainsTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    DomainsAspectRequestV2 req =
        DomainsAspectRequestV2.builder()
            .value(Domains.builder().domains(List.of("my_domain")).build())
            .build();
    assertEquals(datasetApiController.createDomains(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteDomains(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getDomains(testUrn, false).getStatusCode(), HttpStatus.NOT_FOUND);
    assertEquals(datasetApiController.headDomains(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void ownershipTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    OwnershipAspectRequestV2 req =
        OwnershipAspectRequestV2.builder()
            .value(
                Ownership.builder()
                    .owners(
                        List.of(
                            Owner.builder().owner("me").type(OwnershipType.BUSINESS_OWNER).build()))
                    .build())
            .build();
    assertEquals(datasetApiController.createOwnership(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteOwnership(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getOwnership(testUrn, false).getStatusCode(), HttpStatus.NOT_FOUND);
    assertEquals(datasetApiController.headOwnership(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void statusTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    StatusAspectRequestV2 req =
        StatusAspectRequestV2.builder().value(Status.builder().removed(true).build()).build();
    assertEquals(datasetApiController.createStatus(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteStatus(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getStatus(testUrn, false).getStatusCode(), HttpStatus.NOT_FOUND);
    assertEquals(datasetApiController.headStatus(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void globalTagsTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    GlobalTagsAspectRequestV2 req =
        GlobalTagsAspectRequestV2.builder()
            .value(
                GlobalTags.builder()
                    .tags(List.of(TagAssociation.builder().tag("tag").build()))
                    .build())
            .build();
    assertEquals(
        datasetApiController.createGlobalTags(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteGlobalTags(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getGlobalTags(testUrn, false).getStatusCode(), HttpStatus.NOT_FOUND);
    assertEquals(
        datasetApiController.headGlobalTags(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void glossaryTermsTest() {
    final String testUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

    GlossaryTermsAspectRequestV2 req =
        GlossaryTermsAspectRequestV2.builder()
            .value(
                GlossaryTerms.builder()
                    .terms(List.of(GlossaryTermAssociation.builder().urn("term urn").build()))
                    .build())
            .build();
    assertEquals(
        datasetApiController.createGlossaryTerms(testUrn, req).getStatusCode(), HttpStatus.OK);
    assertEquals(datasetApiController.deleteGlossaryTerms(testUrn).getStatusCode(), HttpStatus.OK);
    assertEquals(
        datasetApiController.getGlossaryTerms(testUrn, false).getStatusCode(),
        HttpStatus.NOT_FOUND);
    assertEquals(
        datasetApiController.headGlossaryTerms(testUrn).getStatusCode(), HttpStatus.NOT_FOUND);
  }

  /**
   * The purpose of this test is to ensure no errors when a custom aspect is encountered, not that
   * the custom aspect is processed. The missing piece to support custom aspects is the openapi
   * generated classes for the custom aspects and related request/responses.
   */
  @Test
  public void customModelTest() throws Exception {
    String expectedUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";

    // CHECKSTYLE:OFF
    String body =
        "[\n"
            + "    {\n"
            + "        \"urn\": \""
            + expectedUrn
            + "\",\n"
            + "        \"customDataQualityRules\": [\n"
            + "            {\n"
            + "                \"field\": \"my_event_data\",\n"
            + "                \"isFieldLevel\": false,\n"
            + "                \"type\": \"isNull\",\n"
            + "                \"checkDefinition\": \"n/a\",\n"
            + "                \"url\": \"https://github.com/datahub-project/datahub/blob/master/checks/nonNull.sql\"\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "]";
    // CHECKSTYLE:ON

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/v2/entity/dataset")
                .content(body)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(MockMvcResultMatchers.jsonPath("$.[0].urn").value(expectedUrn));
  }
}
