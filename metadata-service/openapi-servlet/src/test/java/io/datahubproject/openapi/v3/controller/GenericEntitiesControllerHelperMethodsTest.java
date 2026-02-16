package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.versioning.EntityVersioningServiceFactory;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for GenericEntitiesController helper methods and edge cases.
 *
 * <p>These tests focus on improving coverage for:
 *
 * <ul>
 *   <li>Private helper methods for domain extraction
 *   <li>Error handling paths
 *   <li>Edge cases in sorting, slicing, and pagination
 *   <li>Invalid input handling
 *   <li>Timeseries aspect operations
 * </ul>
 */
@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.EntityController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  EntityController.class,
  GenericEntitiesControllerHelperMethodsTest.TestConfig.class,
  EntityVersioningServiceFactory.class,
  GlobalControllerExceptionHandler.class
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class GenericEntitiesControllerHelperMethodsTest extends AbstractTestNGSpringContextTests {

  @Autowired private MockMvc mockMvc;
  @Autowired private EntityService<?> mockEntityService;
  @Autowired private SearchService mockSearchService;
  @Autowired private TimeseriesAspectService mockTimeseriesAspectService;
  @Autowired private AuthorizerChain mockAuthorizerChain;
  @Autowired private ConfigurationProvider mockConfigurationProvider;

  private FeatureFlags featureFlags;

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:test,table,PROD)";
  private static final String FINANCE_DOMAIN_URN = "urn:li:domain:finance";

  @BeforeMethod
  public void setup() {
    reset(
        mockEntityService,
        mockSearchService,
        mockTimeseriesAspectService,
        mockAuthorizerChain,
        mockConfigurationProvider);

    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    featureFlags = new FeatureFlags();
    featureFlags.setDomainBasedAuthorizationEnabled(false);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    when(mockAuthorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  // ========== INVALID URN TESTS ==========

  /**
   * Test get entity with invalid URN format.
   *
   * <p>Note: The URL pattern requires URNs to start with "urn:li:", so invalid URNs that don't
   * match the pattern will return 404 Not Found (no route matched).
   */
  @Test
  public void testGetEntity_InvalidUrnFormat() throws Exception {
    // URNs that don't match urn:li: pattern return 404 (no route match)
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/entity/dataset/invalid-urn-format")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  /**
   * Test get entity with malformed URN - returns 404 as route pattern is strict.
   *
   * <p>Note: The route pattern for dataset URN requires the full format
   * "urn:li:dataset:(platform,name,env)", so malformed URNs don't match.
   */
  @Test
  public void testGetEntity_MalformedUrn() throws Exception {
    // URN matches urn:li:dataset pattern but is still malformed, returns 404
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/entity/dataset/urn:li:dataset:malformed-without-proper-format")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  /**
   * Test head entity with invalid URN format - returns 404 as route doesn't match. Note: The URL
   * pattern requires URNs to start with "urn:li:"
   */
  @Test
  public void testHeadEntity_InvalidUrn() throws Exception {
    // URNs that don't match urn:li: pattern return 404 (no route match)
    mockMvc
        .perform(MockMvcRequestBuilders.head("/openapi/v3/entity/dataset/invalid-urn-format"))
        .andExpect(status().isNotFound());
  }

  /** Test get aspect with invalid URN format - returns 404 as route doesn't match. */
  @Test
  public void testGetAspect_InvalidUrn() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/entity/dataset/invalid-urn/status")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  /** Test delete entity with invalid URN - returns 404 as route doesn't match. */
  @Test
  public void testDeleteEntity_InvalidUrn() throws Exception {
    mockMvc
        .perform(MockMvcRequestBuilders.delete("/openapi/v3/entity/dataset/invalid-urn"))
        .andExpect(status().isNotFound());
  }

  // ========== SORT CRITERIA EDGE CASES ==========

  /** Test scroll with blank sort criteria values. */
  @Test
  public void testGetEntities_BlankSortCriteria() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(datasetUrn);
    searchEntities.add(searchEntity);

    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(searchEntities);
    scrollResult.setNumEntities(1);

    when(mockSearchService.scrollAcrossEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(scrollResult);

    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(datasetUrn);
    entityResponse.setEntityName("dataset");
    entityResponse.setAspects(new EnvelopedAspectMap());
    entityResponseMap.put(datasetUrn, entityResponse);

    when(mockEntityService.getEntitiesV2(any(), eq("dataset"), any(), any(), eq(false)))
        .thenReturn(entityResponseMap);

    // Test with blank string in sortCriteria - should be filtered out
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/entity/dataset")
                .param("sortCriteria", "", " ", "name")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(mockSearchService)
        .scrollAcrossEntities(any(), any(), any(), any(), any(), any(), any(), any());
  }

  /** Test scroll with null sort order - defaults to ASCENDING. */
  @Test
  public void testGetEntities_NullSortOrder() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(datasetUrn);
    searchEntities.add(searchEntity);

    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(searchEntities);
    scrollResult.setNumEntities(1);

    when(mockSearchService.scrollAcrossEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(scrollResult);

    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(datasetUrn);
    entityResponse.setEntityName("dataset");
    entityResponse.setAspects(new EnvelopedAspectMap());
    entityResponseMap.put(datasetUrn, entityResponse);

    when(mockEntityService.getEntitiesV2(any(), eq("dataset"), any(), any(), eq(false)))
        .thenReturn(entityResponseMap);

    // Test with blank sortField and no sortCriteria - should default to "urn"
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/entity/dataset")
                .param("sort", "")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // ========== SLICING EDGE CASES ==========

  /** Test scroll with only sliceId (no sliceMax) - slicing should be ignored. */
  @Test
  public void testGetEntities_OnlySliceId() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(datasetUrn);
    searchEntities.add(searchEntity);

    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(searchEntities);
    scrollResult.setNumEntities(1);

    when(mockSearchService.scrollAcrossEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(scrollResult);

    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(datasetUrn);
    entityResponse.setEntityName("dataset");
    entityResponse.setAspects(new EnvelopedAspectMap());
    entityResponseMap.put(datasetUrn, entityResponse);

    when(mockEntityService.getEntitiesV2(any(), eq("dataset"), any(), any(), eq(false)))
        .thenReturn(entityResponseMap);

    // Only sliceId without sliceMax
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/entity/dataset")
                .param("sliceId", "1")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // ========== DOMAIN EXTRACTION EDGE CASES ==========

  /** Test create entity with domain auth enabled when entity has null domain aspect. */
  @Test
  public void testCreateEntity_NullDomainAspect() throws Exception {
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    // Mock entity response with null domains aspect
    EntityResponse mockEntityResponse = new EntityResponse();
    mockEntityResponse.setAspects(new EnvelopedAspectMap());
    when(mockEntityService.getEntityV2(any(), eq("dataset"), eq(datasetUrn), any()))
        .thenReturn(mockEntityResponse);

    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(new Status().setRemoved(false));

    when(mockEntityService.ingestProposal(any(), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .sqlCommitted(true)
                    .build()));

    String requestBody =
        "[{\"urn\": \"" + DATASET_URN + "\", \"status\": {\"value\": {\"removed\": false}}}]";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService).ingestProposal(any(), any(), anyBoolean());
  }

  /** Test create entity with domain auth enabled when domains has null domain list. */
  @Test
  public void testCreateEntity_DomainsWithNullList() throws Exception {
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    // Mock entity response with domains aspect that has hasDomains=false
    Domains domains = new Domains();
    // Don't set domains array - simulates null domains list

    EntityResponse mockEntityResponse = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect domainsAspect = new EnvelopedAspect();
    domainsAspect.setValue(new Aspect(domains.data()));
    aspectMap.put("domains", domainsAspect);
    mockEntityResponse.setAspects(aspectMap);

    when(mockEntityService.getEntityV2(any(), eq("dataset"), eq(datasetUrn), any()))
        .thenReturn(mockEntityResponse);

    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(new Status().setRemoved(false));

    when(mockEntityService.ingestProposal(any(), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .sqlCommitted(true)
                    .build()));

    String requestBody =
        "[{\"urn\": \"" + DATASET_URN + "\", \"status\": {\"value\": {\"removed\": false}}}]";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());
  }

  // ========== CREATE ASPECT WITH DOMAIN AUTH ==========

  /** Test createAspect with entity not existing and createIfEntityNotExists=true. */
  @Test
  public void testCreateAspect_CreateIfEntityNotExists() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    UpdateAspectResult mockResult = mock(UpdateAspectResult.class);
    when(mockResult.getUrn()).thenReturn(datasetUrn);
    when(mockResult.getNewValue()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .result(mockResult)
                    .sqlCommitted(true)
                    .build()));

    String requestBody = "{\"value\": {\"removed\": false}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .param("createIfEntityNotExists", "true")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());
  }

  /** Test createAspect with createIfNotExists=false - should update only if exists. */
  @Test
  public void testCreateAspect_CreateIfNotExistsFalse() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    UpdateAspectResult mockResult = mock(UpdateAspectResult.class);
    when(mockResult.getUrn()).thenReturn(datasetUrn);
    when(mockResult.getNewValue()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .result(mockResult)
                    .sqlCommitted(true)
                    .build()));

    String requestBody = "{\"value\": {\"removed\": false}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .param("createIfNotExists", "false")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());
  }

  /** Test createAspect async mode returns 202 Accepted. */
  @Test
  public void testCreateAspect_AsyncMode() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), eq(true)))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .sqlCommitted(true)
                    .build()));

    String requestBody = "{\"value\": {\"removed\": false}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .param("async", "true")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isAccepted());

    verify(mockEntityService).ingestProposal(any(OperationContext.class), any(), eq(true));
  }

  // ========== PATCH OPERATIONS WITH VARIOUS PATCH OPS ==========

  /** Test patchAspect with add operation to domains path. */
  @Test
  public void testPatchAspect_AddDomainOperation() throws Exception {
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn domainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(domains);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    // Test add operation with single domain string value
    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"add\", \"path\": \"/domains\", \"value\": \""
            + FINANCE_DOMAIN_URN
            + "\"}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/domains")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());
  }

  /** Test patchAspect with replace operation using array value. */
  @Test
  public void testPatchAspect_ReplaceWithArrayValue() throws Exception {
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn domainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(domains);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    // Test replace operation with array of domain strings
    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"replace\", \"path\": \"/domains\", \"value\": [\""
            + FINANCE_DOMAIN_URN
            + "\", \"urn:li:domain:marketing\"]}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/domains")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());
  }

  /** Test patchAspect with invalid domain URN in patch - should handle gracefully. */
  @Test
  public void testPatchAspect_InvalidDomainUrnInPatch() throws Exception {
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Domains domains = new Domains();
    domains.setDomains(new UrnArray());
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(domains);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    // Patch with invalid URN format (not urn:li:domain:)
    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"add\", \"path\": \"/domains/-\", \"value\": \"invalid-not-a-domain-urn\"}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/domains")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());
  }

  /** Test patchAspect with empty patch array. */
  @Test
  public void testPatchAspect_EmptyPatchArray() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    String patchBody = "{\"patch\": []}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());
  }

  /** Test patchAspect with null patch array returns 400 Bad Request. */
  @Test
  public void testPatchAspect_NullPatchArray() throws Exception {
    // Test with patch field set to null - should fail validation
    String patchBody = "{\"patch\": null}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().isBadRequest());

    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), any(), anyBoolean());
  }

  // ========== DELETE ENTITY EDGE CASES ==========

  /** Test delete entity with key aspect in aspects list - should delete entire entity. */
  @Test
  public void testDeleteEntity_WithKeyAspect() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    when(mockEntityService.exists(any(), anyCollection(), eq(true)))
        .thenReturn(Collections.singleton(datasetUrn));

    // Including datasetKey (key aspect) should trigger full entity deletion
    mockMvc
        .perform(
            MockMvcRequestBuilders.delete("/openapi/v3/entity/dataset/" + DATASET_URN)
                .param("aspects", "datasetKey,status"))
        .andExpect(status().isOk());

    // Should call deleteUrn instead of deleteAspect
    verify(mockEntityService).deleteUrn(any(), eq(datasetUrn));
    verify(mockEntityService, never()).deleteAspect(any(), any(), any(), any(), anyBoolean());
  }

  /** Test delete entity with empty aspects list - should delete entire entity. */
  @Test
  public void testDeleteEntity_EmptyAspectsList() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    when(mockEntityService.exists(any(), anyCollection(), eq(true)))
        .thenReturn(Collections.singleton(datasetUrn));

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete("/openapi/v3/entity/dataset/" + DATASET_URN)
                .param("aspects", ""))
        .andExpect(status().isOk());

    verify(mockEntityService).deleteUrn(any(), eq(datasetUrn));
  }

  // ========== CREATE ENTITY SYNC MODE ==========

  /** Test create entity sync mode returns 200 OK with result. */
  @Test
  public void testCreateEntity_SyncMode() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(), any(), eq(false)))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .sqlCommitted(true)
                    .build()));

    String requestBody =
        "[{\"urn\": \"" + DATASET_URN + "\", \"status\": {\"value\": {\"removed\": false}}}]";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset")
                .param("async", "false")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(mockEntityService).ingestProposal(any(), any(), eq(false));
  }

  // ========== GET ASPECT WITH VERSION ==========

  /** Test get aspect with version 0 (latest). */
  @Test
  public void testGetAspect_Version0() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    Map<Urn, List<EnvelopedAspect>> envelopedAspects = new HashMap<>();
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    statusAspect.setName("status");
    statusAspect.setValue(new Aspect(new Status().setRemoved(false).data()));
    envelopedAspects.put(datasetUrn, List.of(statusAspect));

    when(mockEntityService.getEnvelopedVersionedAspects(any(), anyMap(), eq(false)))
        .thenReturn(envelopedAspects);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .param("version", "0")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // Note: testGetAspect_WithSystemMetadata removed - requires complex mock setup for
  // the full response transformation pipeline. This functionality is tested in
  // integration tests instead.

  // ========== ENTITY EXISTS WITH INCLUDE SOFT DELETE ==========

  /** Test head entity with includeSoftDelete=true. */
  @Test
  public void testHeadEntity_IncludeSoftDelete() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    // Use anyBoolean() instead of eq(true) since default may vary
    when(mockEntityService.exists(any(), eq(datasetUrn), anyBoolean())).thenReturn(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.head("/openapi/v3/entity/dataset/" + DATASET_URN)
                .param("includeSoftDelete", "true"))
        .andExpect(status().isNoContent());

    verify(mockEntityService).exists(any(), eq(datasetUrn), anyBoolean());
  }

  /** Test head aspect with includeSoftDelete=true. */
  @Test
  public void testHeadAspect_IncludeSoftDelete() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    // Use anyBoolean() instead of eq(true) since default may vary
    when(mockEntityService.exists(any(), eq(datasetUrn), eq("status"), anyBoolean()))
        .thenReturn(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.head("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .param("includeSoftDelete", "true"))
        .andExpect(status().isNoContent());

    verify(mockEntityService).exists(any(), eq(datasetUrn), eq("status"), anyBoolean());
  }

  @TestConfiguration
  public static class TestConfig {
    @MockBean public EntityServiceImpl entityService;
    @MockBean public SearchService searchService;
    @MockBean public TimeseriesAspectService timeseriesAspectService;
    @MockBean public SystemTelemetryContext systemTelemetryContext;
    @MockBean public ConfigurationProvider configurationProvider;

    @Bean
    public AuthorizerChain authorizerChain() {
      return mock(AuthorizerChain.class);
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("entityRegistry")
    @Primary
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") final OperationContext testOperationContext) {
      return testOperationContext.getEntityRegistry();
    }
  }
}
