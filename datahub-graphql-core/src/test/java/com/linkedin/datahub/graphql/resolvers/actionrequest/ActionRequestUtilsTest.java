package com.linkedin.datahub.graphql.resolvers.actionrequest;

import static com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_REJECTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.actionrequest.DomainProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.StructuredPropertyProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.ai.InferenceMetadata;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestOrigin;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.Arrays;
import java.util.Collections;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Test class to verify mapping logic in {@link ActionRequestUtils}. */
public class ActionRequestUtilsTest {

  @Mock private QueryContext mockContext;

  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void teardown() throws Exception {
    mocks.close();
  }

  /**
   * Helper method to build an Entity containing an ActionRequestSnapshot with the provided aspects.
   */
  private Entity buildEntity(ActionRequestAspect... aspects) {
    ActionRequestSnapshot snapshot = new ActionRequestSnapshot();
    // Arbitrary URN for testing
    Urn testUrn = UrnUtils.getUrn("urn:li:actionRequest:123");
    snapshot.setUrn(testUrn);
    snapshot.setAspects(new ActionRequestAspectArray(Arrays.asList(aspects)));

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    return entity;
  }

  @Test
  public void testMapActionRequestWithLegacyTagProposal() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a TagProposal
    // -----------------------------------------
    TagProposal tagProposal = new TagProposal();
    tagProposal.setTag(UrnUtils.getUrn("urn:li:tag:myTestTag"));
    tagProposal.setTags(new UrnArray(Collections.emptyList()));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setTagProposal(tagProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(TAG_ASSOCIATION.toString());
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    infoAspect.setCreated(12345L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:datahub"));
    infoAspect.setParams(requestParams);
    infoAspect.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.MANUAL);
    infoAspect.setInferenceMetadata(
        new InferenceMetadata().setVersion(1).setLastInferredAt(98765L));
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_PENDING);
    statusAspect.setResult("");
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), TAG_ASSOCIATION);
    assertEquals(request.getStatus(), ActionRequestStatus.PENDING);
    assertNull(request.getResult()); // Because statusAspect had empty string for result
    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getTagProposal());
    assertEquals(request.getParams().getTagProposal().getTag().getUrn(), "urn:li:tag:myTestTag");
    assertEquals(request.getCreated().getTime(), Long.valueOf(12345L));
    assertEquals(request.getCreated().getActor().getUrn(), "urn:li:corpuser:datahub");
    assertEquals(request.getOrigin(), ActionRequestOrigin.MANUAL);
    assertNotNull(request.getInferenceMetadata());
    assertEquals(request.getInferenceMetadata().getVersion(), 1);
    assertEquals(request.getInferenceMetadata().getLastInferredAt(), Long.valueOf(98765L));
  }

  @Test
  public void testMapActionRequestWithTagsProposal() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a TagProposal
    // -----------------------------------------
    TagProposal tagProposal = new TagProposal();
    tagProposal.setTags(
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn("urn:li:tag:myTestTag1"),
                UrnUtils.getUrn("urn:li:tag:myTestTag2"))));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setTagProposal(tagProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(TAG_ASSOCIATION.toString());
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    infoAspect.setCreated(12345L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:datahub"));
    infoAspect.setParams(requestParams);
    infoAspect.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.MANUAL);
    infoAspect.setInferenceMetadata(
        new InferenceMetadata().setVersion(1).setLastInferredAt(98765L));
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_PENDING);
    statusAspect.setResult("");
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), TAG_ASSOCIATION);
    assertEquals(request.getStatus(), ActionRequestStatus.PENDING);
    assertNull(request.getResult()); // Because statusAspect had empty string for result
    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getTagProposal());
    assertEquals(
        request.getParams().getTagProposal().getTags().get(0).getUrn(), "urn:li:tag:myTestTag1");
    assertEquals(
        request.getParams().getTagProposal().getTags().get(1).getUrn(), "urn:li:tag:myTestTag2");
    assertEquals(request.getCreated().getTime(), Long.valueOf(12345L));
    assertEquals(request.getCreated().getActor().getUrn(), "urn:li:corpuser:datahub");
    assertEquals(request.getOrigin(), ActionRequestOrigin.MANUAL);
    assertNotNull(request.getInferenceMetadata());
    assertEquals(request.getInferenceMetadata().getVersion(), 1);
    assertEquals(request.getInferenceMetadata().getLastInferredAt(), Long.valueOf(98765L));
  }

  @Test
  public void testMapActionRequestWithLegacyTermProposal() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a GlossaryTermProposal
    // -----------------------------------------
    GlossaryTermProposal termProposal = new GlossaryTermProposal();
    // "term" is the legacy single-term field
    termProposal.setGlossaryTerm(UrnUtils.getUrn("urn:li:glossaryTerm:mySingleTerm"));
    termProposal.setGlossaryTerms(new UrnArray(Collections.emptyList()));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setGlossaryTermProposal(termProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(ACTION_REQUEST_TYPE_TERM_PROPOSAL);
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    infoAspect.setCreated(23456L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:testUser"));
    infoAspect.setParams(requestParams);
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    statusAspect.setResult(ACTION_REQUEST_RESULT_ACCEPTED);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), ActionRequestType.TERM_ASSOCIATION);
    assertEquals(request.getStatus(), ActionRequestStatus.COMPLETED);
    // This time, statusAspect had "SUCCESS" for result
    assertNotNull(request.getResult());
    assertEquals(request.getResult(), ActionRequestResult.ACCEPTED);
    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getGlossaryTermProposal());
    assertEquals(
        request.getParams().getGlossaryTermProposal().getGlossaryTerm().getUrn(),
        "urn:li:glossaryTerm:mySingleTerm");
    assertEquals(request.getCreated().getTime(), Long.valueOf(23456L));
    assertEquals(request.getCreated().getActor().getUrn(), "urn:li:corpuser:testUser");
  }

  @Test
  public void testMapActionRequestWithTermProposal() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a GlossaryTermProposal
    // -----------------------------------------
    GlossaryTermProposal termProposal = new GlossaryTermProposal();
    // "term" is the legacy single-term field
    termProposal.setGlossaryTerms(
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn("urn:li:glossaryTerm:myTerm1"),
                UrnUtils.getUrn("urn:li:glossaryTerm:myTerm2"))));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setGlossaryTermProposal(termProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(ACTION_REQUEST_TYPE_TERM_PROPOSAL);
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    infoAspect.setCreated(23456L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:testUser"));
    infoAspect.setParams(requestParams);
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    statusAspect.setResult(ACTION_REQUEST_RESULT_ACCEPTED);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), ActionRequestType.TERM_ASSOCIATION);
    assertEquals(request.getStatus(), ActionRequestStatus.COMPLETED);
    // This time, statusAspect had "SUCCESS" for result
    assertNotNull(request.getResult());
    assertEquals(request.getResult(), ActionRequestResult.ACCEPTED);
    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getGlossaryTermProposal());
    assertEquals(
        request.getParams().getGlossaryTermProposal().getGlossaryTerms().get(0).getUrn(),
        "urn:li:glossaryTerm:myTerm1");
    assertEquals(
        request.getParams().getGlossaryTermProposal().getGlossaryTerms().get(1).getUrn(),
        "urn:li:glossaryTerm:myTerm2");
    assertEquals(request.getCreated().getTime(), Long.valueOf(23456L));
    assertEquals(request.getCreated().getActor().getUrn(), "urn:li:corpuser:testUser");
  }

  @Test
  public void testMapActionRequestWithStructuredPropertyProposal() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a StructuredPropertyProposal
    // -----------------------------------------
    StructuredPropertyProposal structPropProposal = new StructuredPropertyProposal();

    // Set multiple properties
    StructuredPropertyValueAssignment property1 = new StructuredPropertyValueAssignment();
    property1.setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:test"));
    property1.setValues(
        new PrimitivePropertyValueArray(
            ImmutableList.of(
                PrimitivePropertyValue.create("value1"), PrimitivePropertyValue.create("value2"))));

    StructuredPropertyValueAssignment property2 = new StructuredPropertyValueAssignment();
    property2.setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:test2"));
    property2.setValues(
        new PrimitivePropertyValueArray(
            ImmutableList.of(
                PrimitivePropertyValue.create(10.0), PrimitivePropertyValue.create(10.1))));

    structPropProposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(ImmutableList.of(property1, property2)));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setStructuredPropertyProposal(structPropProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myAnotherDataset,PROD)");
    infoAspect.setCreated(34567L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:structuredUser"));
    infoAspect.setParams(requestParams);
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_PENDING);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), ActionRequestType.STRUCTURED_PROPERTY_ASSOCIATION);
    assertEquals(request.getStatus(), ActionRequestStatus.PENDING);
    // Because result was an empty string
    assertNull(request.getResult());
    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getStructuredPropertyProposal());

    // Check the 2 assignments
    assertEquals(
        request.getParams().getStructuredPropertyProposal().getStructuredProperties().size(), 2);

    // Validate property 1
    assertEquals(
        request
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredProperties()
            .get(0)
            .getStructuredProperty()
            .getUrn(),
        "urn:li:structuredProperty:test");
    assertEquals(
        request
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredProperties()
            .get(0)
            .getValues()
            .size(),
        2);
    assertEquals(
        request
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredProperties()
            .get(0)
            .getAssociatedUrn(),
        infoAspect.getResource());
    assertEquals(
        request
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredProperties()
            .get(1)
            .getStructuredProperty()
            .getUrn(),
        "urn:li:structuredProperty:test2");
    assertEquals(
        request
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredProperties()
            .get(1)
            .getValues()
            .size(),
        2);
    assertEquals(
        request
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredProperties()
            .get(1)
            .getAssociatedUrn(),
        infoAspect.getResource());
    assertEquals(request.getCreated().getTime(), Long.valueOf(34567L));
    assertEquals(request.getCreated().getActor().getUrn(), "urn:li:corpuser:structuredUser");
  }

  @Test
  public void testMapActionRequestWithDescriptionProposalOnly() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a DescriptionProposal
    // -----------------------------------------
    DescriptionProposal descriptionProposal = new DescriptionProposal();
    descriptionProposal.setDescription("New description for my dataset field.");

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setUpdateDescriptionProposal(descriptionProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL);
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myUpdatedDataset,PROD)");
    infoAspect.setCreated(99999L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:updateUser"));
    infoAspect.setParams(requestParams);
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    statusAspect.setResult(ACTION_REQUEST_RESULT_REJECTED);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), ActionRequestType.UPDATE_DESCRIPTION);
    assertEquals(request.getStatus(), ActionRequestStatus.COMPLETED);
    assertEquals(request.getResult(), ActionRequestResult.REJECTED);

    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getUpdateDescriptionProposal());
    assertEquals(
        request.getParams().getUpdateDescriptionProposal().getDescription(),
        "New description for my dataset field.");
    assertEquals(request.getCreated().getTime(), Long.valueOf(99999L));
    assertEquals(request.getCreated().getActor().getUrn(), "urn:li:corpuser:updateUser");
  }

  @Test
  public void testMapActionRequestWithDomainProposal() throws Exception {
    // -----------------------------------------
    // Given: An ActionRequestInfo with a Domain
    // -----------------------------------------
    DomainProposal domainProposal = new DomainProposal();
    domainProposal.setDomains(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:domain:test"))));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setDomainProposal(domainProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL);
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myAnotherDataset,PROD)");
    infoAspect.setCreated(34567L);
    infoAspect.setCreatedBy(Urn.createFromString("urn:li:corpuser:structuredUser"));
    infoAspect.setParams(requestParams);
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);

    com.linkedin.actionrequest.ActionRequestStatus statusAspect =
        new com.linkedin.actionrequest.ActionRequestStatus();
    statusAspect.setStatus(ACTION_REQUEST_STATUS_PENDING);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    Entity entity = buildEntity(aspect1, aspect2);

    // -----------------------------------------
    // When: We invoke the mapper
    // -----------------------------------------
    ActionRequest request =
        ActionRequestUtils.mapActionRequest(
            mockContext, entity.getValue().getActionRequestSnapshot());

    // -----------------------------------------
    // Then: The fields in the mapped request should match
    // -----------------------------------------
    assertNotNull(request);
    assertEquals(request.getType(), ActionRequestType.DOMAIN_ASSOCIATION);
    assertEquals(request.getStatus(), ActionRequestStatus.PENDING);
    // Because result was an empty string
    assertNull(request.getResult());
    assertNotNull(request.getParams());
    assertNotNull(request.getParams().getDomainProposal());

    // Check the 2 assignments
    assertEquals(
        request.getParams().getDomainProposal().getDomain().getUrn(), "urn:li:domain:test");
  }
}
