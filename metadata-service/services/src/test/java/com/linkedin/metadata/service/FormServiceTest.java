package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Covers the batching contract of the form assign/unassign paths: the number of calls made to the
 * entity client must not scale with the number of entities in the batch.
 */
public class FormServiceTest {

  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:testForm");
  private static final String TEST_PROMPT_ID = "prompt-1";
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private static List<Urn> datasetUrns(int count) {
    return IntStream.range(0, count)
        .mapToObj(
            i ->
                UrnUtils.getUrn(
                    String.format("urn:li:dataset:(urn:li:dataPlatform:kafka,table%d,PROD)", i)))
        .collect(Collectors.toList());
  }

  @Test
  public void testBatchAssignFormDoesNotFanOutPerEntity() throws Exception {
    final List<Urn> entityUrns = datasetUrns(10);
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, Map.of());

    new FormService(mockClient).batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    // One existence check, one aspect read, and one form definition read for the whole batch.
    verify(mockClient, times(1)).filterExistingUrns(eq(OP_CONTEXT), anyCollection());
    verify(mockClient, times(1))
        .batchGetV2(eq(OP_CONTEXT), eq(DATASET_ENTITY_NAME), anySet(), anySet());
    verify(mockClient, times(1))
        .getV2(eq(OP_CONTEXT), eq(FORM_ENTITY_NAME), eq(TEST_FORM_URN), anySet());
    // No per-entity reads or writes.
    verify(mockClient, never()).getV2(eq(OP_CONTEXT), eq(DATASET_ENTITY_NAME), any(), any());
    // The single exists() call is the form itself; entities go through filterExistingUrns.
    verify(mockClient, times(1)).exists(eq(OP_CONTEXT), eq(TEST_FORM_URN));
    verify(mockClient, never()).ingestProposal(any(), any());
    verify(mockClient, never()).ingestProposal(any(), any(), anyBoolean());

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), entityUrns.size());
    assertEquals(
        mcps.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet()),
        new HashSet<>(entityUrns));

    final Forms assigned = deserializeForms(mcps.get(0));
    assertEquals(assigned.getIncompleteForms().size(), 1);
    assertEquals(assigned.getIncompleteForms().get(0).getUrn(), TEST_FORM_URN);
    assertEquals(assigned.getIncompleteForms().get(0).getIncompletePrompts().size(), 1);
    assertEquals(
        assigned.getIncompleteForms().get(0).getIncompletePrompts().get(0).getId(), TEST_PROMPT_ID);
  }

  @Test
  public void testBatchAssignFormSkipsAlreadyAssignedEntities() throws Exception {
    final List<Urn> entityUrns = datasetUrns(3);
    final Map<Urn, Forms> existing =
        Map.of(
            entityUrns.get(0), formsWithIncomplete(TEST_FORM_URN), entityUrns.get(1), emptyForms());
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient).batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    // Entity 0 already has the form, so only entities 1 and 2 produce proposals.
    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(
        mcps.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet()),
        Set.of(entityUrns.get(1), entityUrns.get(2)));
  }

  @Test
  public void testBatchUnassignFormOnlyProposesForAssignedEntities() throws Exception {
    final List<Urn> entityUrns = datasetUrns(3);
    final Map<Urn, Forms> existing =
        Map.of(
            entityUrns.get(0),
            formsWithIncomplete(TEST_FORM_URN),
            entityUrns.get(1),
            formsWithIncomplete(UrnUtils.getUrn("urn:li:form:otherForm")));
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient).batchUnassignFormForEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    verify(mockClient, times(1)).filterExistingUrns(eq(OP_CONTEXT), anyCollection());
    verify(mockClient, times(1))
        .batchGetV2(eq(OP_CONTEXT), eq(DATASET_ENTITY_NAME), anySet(), anySet());

    // Only entity 0 actually has the form assigned.
    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), 1);
    assertEquals(mcps.get(0).getEntityUrn(), entityUrns.get(0));
    assertTrue(deserializeForms(mcps.get(0)).getIncompleteForms().isEmpty());
  }

  @Test
  public void testBatchAssignFormGroupsReadsByEntityType() throws Exception {
    final List<Urn> entityUrns =
        ImmutableList.of(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,table0,PROD)"),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,table1,PROD)"),
            UrnUtils.getUrn("urn:li:chart:(looker,chart0)"),
            UrnUtils.getUrn("urn:li:chart:(looker,chart1)"));
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, Map.of());

    new FormService(mockClient).batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    // Two entity types in the batch means two reads, not four.
    verify(mockClient, times(1))
        .batchGetV2(eq(OP_CONTEXT), eq(DATASET_ENTITY_NAME), anySet(), anySet());
    verify(mockClient, times(1))
        .batchGetV2(eq(OP_CONTEXT), eq(CHART_ENTITY_NAME), anySet(), anySet());
    assertEquals(captureSingleIngestBatch(mockClient).size(), entityUrns.size());
  }

  @Test
  public void testBatchAssignFormRejectsMissingEntities() throws Exception {
    final List<Urn> entityUrns = datasetUrns(3);
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, Map.of());
    // Only the first entity exists.
    when(mockClient.filterExistingUrns(eq(OP_CONTEXT), anyCollection()))
        .thenReturn(Set.of(entityUrns.get(0)));

    final FormService service = new FormService(mockClient);
    assertThrows(
        RuntimeException.class,
        () -> service.batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN));

    verify(mockClient, never()).batchIngestProposals(any(), anyCollection(), anyBoolean());
  }

  @Test
  public void testBatchSetFormPromptIncompleteSkipsEntitiesWithoutFormsAspect() throws Exception {
    final List<Urn> entityUrns = datasetUrns(2);
    // Only entity 0 has a forms aspect at all; entity 1 must be skipped, not fail the batch.
    final Map<Urn, Forms> existing =
        Map.of(entityUrns.get(0), completedFormsWith(TEST_FORM_URN, TEST_PROMPT_ID));
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient)
        .batchSetFormPromptIncomplete(OP_CONTEXT, entityUrns, TEST_FORM_URN, TEST_PROMPT_ID);

    verify(mockClient, times(1))
        .batchGetV2(eq(OP_CONTEXT), eq(DATASET_ENTITY_NAME), anySet(), anySet());

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), 1);
    assertEquals(mcps.get(0).getEntityUrn(), entityUrns.get(0));
  }

  @Test
  public void testBatchUnassignFormRemovesFromCompletedForms() throws Exception {
    final List<Urn> entityUrns = datasetUrns(1);
    final Map<Urn, Forms> existing =
        Map.of(entityUrns.get(0), completedFormsWith(TEST_FORM_URN, TEST_PROMPT_ID));
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient).batchUnassignFormForEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), 1);
    final Forms result = deserializeForms(mcps.get(0));
    assertTrue(result.getCompletedForms().isEmpty());
    assertTrue(result.getIncompleteForms().isEmpty());
  }

  @Test
  public void testBatchAssignFormWithNothingToChangeSkipsIngestEntirely() throws Exception {
    final List<Urn> entityUrns = datasetUrns(1);
    // The only entity already has the form, so no proposals are produced.
    final Map<Urn, Forms> existing = Map.of(entityUrns.get(0), formsWithIncomplete(TEST_FORM_URN));
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient).batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    verify(mockClient, never()).batchIngestProposals(any(), anyCollection(), anyBoolean());
  }

  @Test
  public void testBatchAssignFormFailsWhenExistenceCheckErrors() throws Exception {
    final List<Urn> entityUrns = datasetUrns(2);
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, Map.of());
    when(mockClient.filterExistingUrns(eq(OP_CONTEXT), anyCollection()))
        .thenThrow(new RuntimeException("primary store unavailable"));

    final FormService service = new FormService(mockClient);
    assertThrows(
        RuntimeException.class,
        () -> service.batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN));

    verify(mockClient, never()).batchIngestProposals(any(), anyCollection(), anyBoolean());
  }

  @Test
  public void testBatchSetFormPromptIncompleteSkipsEntitiesWithoutThatForm() throws Exception {
    final List<Urn> entityUrns = datasetUrns(2);
    // Entity 1 has a forms aspect, but for a different form: it must be skipped.
    final Map<Urn, Forms> existing =
        Map.of(
            entityUrns.get(0),
            completedFormsWith(TEST_FORM_URN, TEST_PROMPT_ID),
            entityUrns.get(1),
            formsWithIncomplete(UrnUtils.getUrn("urn:li:form:otherForm")));
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient)
        .batchSetFormPromptIncomplete(OP_CONTEXT, entityUrns, TEST_FORM_URN, TEST_PROMPT_ID);

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), 1);
    assertEquals(mcps.get(0).getEntityUrn(), entityUrns.get(0));
  }

  @Test
  public void testBatchAssignFormSkipsUnusableAspectAndKeepsRest() throws Exception {
    final List<Urn> entityUrns = datasetUrns(3);
    // Entity 1's stored aspect cannot be read as a Forms record; it alone must be skipped.
    final Map<Urn, Forms> existing = Map.of(entityUrns.get(1), malformedForms());
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient).batchAssignFormToEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(
        mcps.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet()),
        Set.of(entityUrns.get(0), entityUrns.get(2)));
  }

  @Test
  public void testBatchUnassignFormSkipsUnusableAspectAndKeepsRest() throws Exception {
    final List<Urn> entityUrns = datasetUrns(2);
    final Map<Urn, Forms> existing =
        Map.of(
            entityUrns.get(0),
            formsWithIncomplete(TEST_FORM_URN),
            entityUrns.get(1),
            malformedForms());
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient).batchUnassignFormForEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), 1);
    assertEquals(mcps.get(0).getEntityUrn(), entityUrns.get(0));
  }

  @Test
  public void testBatchSetFormPromptIncompleteSkipsUnusableAspectAndKeepsRest() throws Exception {
    final List<Urn> entityUrns = datasetUrns(2);
    final Map<Urn, Forms> existing =
        Map.of(
            entityUrns.get(0),
            completedFormsWith(TEST_FORM_URN, TEST_PROMPT_ID),
            entityUrns.get(1),
            malformedForms());
    final SystemEntityClient mockClient = mockEntityClient(entityUrns, existing);

    new FormService(mockClient)
        .batchSetFormPromptIncomplete(OP_CONTEXT, entityUrns, TEST_FORM_URN, TEST_PROMPT_ID);

    final List<MetadataChangeProposal> mcps = captureSingleIngestBatch(mockClient);
    assertEquals(mcps.size(), 1);
    assertEquals(mcps.get(0).getEntityUrn(), entityUrns.get(0));
  }

  @Test
  public void testBatchUnassignFormLeavesOtherFormsInPlace() throws Exception {
    final Urn otherForm = UrnUtils.getUrn("urn:li:form:otherForm");
    final List<Urn> entityUrns = datasetUrns(1);
    final Forms forms = completedFormsWith(TEST_FORM_URN, TEST_PROMPT_ID);
    forms.getCompletedForms().add(new FormAssociation().setUrn(otherForm));
    final SystemEntityClient mockClient =
        mockEntityClient(entityUrns, Map.of(entityUrns.get(0), forms));

    new FormService(mockClient).batchUnassignFormForEntities(OP_CONTEXT, entityUrns, TEST_FORM_URN);

    final Forms result = deserializeForms(captureSingleIngestBatch(mockClient).get(0));
    assertEquals(
        result.getCompletedForms().stream()
            .map(FormAssociation::getUrn)
            .collect(Collectors.toList()),
        List.of(otherForm));
  }

  /**
   * A stored aspect that cannot be read as a Forms record, standing in for any per-entity
   * deserialization failure: the required incompleteForms / completedForms arrays are absent.
   */
  private static Forms malformedForms() {
    return new Forms();
  }

  /**
   * incompleteForms and completedForms are required in Forms.pdl, so a stored aspect always has
   * both.
   */
  private static Forms emptyForms() {
    return new Forms()
        .setIncompleteForms(new FormAssociationArray())
        .setCompletedForms(new FormAssociationArray());
  }

  private static Forms formsWithIncomplete(@javax.annotation.Nonnull final Urn formUrn) {
    return new Forms()
        .setIncompleteForms(
            new FormAssociationArray(ImmutableList.of(new FormAssociation().setUrn(formUrn))))
        .setCompletedForms(new FormAssociationArray());
  }

  private static Forms completedFormsWith(
      @javax.annotation.Nonnull final Urn formUrn,
      @javax.annotation.Nonnull final String promptId) {
    final FormAssociation association = new FormAssociation().setUrn(formUrn);
    association.setCompletedPrompts(
        new com.linkedin.common.FormPromptAssociationArray(
            ImmutableList.of(
                new com.linkedin.common.FormPromptAssociation()
                    .setId(promptId)
                    .setLastModified(
                        new com.linkedin.common.AuditStamp()
                            .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                            .setTime(0L)))));
    association.setIncompletePrompts(new com.linkedin.common.FormPromptAssociationArray());
    return new Forms()
        .setCompletedForms(new FormAssociationArray(ImmutableList.of(association)))
        .setIncompleteForms(new FormAssociationArray());
  }

  private static Forms deserializeForms(final MetadataChangeProposal mcp) {
    return GenericRecordUtils.deserializeAspect(
        mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Forms.class);
  }

  @SuppressWarnings("unchecked")
  private static List<MetadataChangeProposal> captureSingleIngestBatch(
      final SystemEntityClient mockClient) throws Exception {
    final ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockClient, times(1)).batchIngestProposals(eq(OP_CONTEXT), captor.capture(), eq(false));
    return captor.getValue();
  }

  /**
   * Mocks a client where every urn in {@code entityUrns} exists, the form definition has a single
   * prompt, and {@code existingForms} supplies the forms aspect for the entities that have one.
   */
  private static SystemEntityClient mockEntityClient(
      final List<Urn> entityUrns, final Map<Urn, Forms> existingForms) throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);

    when(mockClient.exists(eq(OP_CONTEXT), eq(TEST_FORM_URN))).thenReturn(true);
    when(mockClient.filterExistingUrns(eq(OP_CONTEXT), anyCollection()))
        .thenReturn(new HashSet<>(entityUrns));

    final FormInfo formInfo =
        new FormInfo()
            .setName("Test Form")
            .setPrompts(
                new FormPromptArray(ImmutableList.of(new FormPrompt().setId(TEST_PROMPT_ID))));
    when(mockClient.getV2(
            eq(OP_CONTEXT),
            eq(FORM_ENTITY_NAME),
            eq(TEST_FORM_URN),
            eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse(TEST_FORM_URN, FORM_INFO_ASPECT_NAME, formInfo.data()));

    // Mirrors the real client: every existing urn comes back with a response, and the forms
    // aspect is simply absent from it when the entity has none.
    when(mockClient.batchGetV2(eq(OP_CONTEXT), anyString(), anySet(), anySet()))
        .thenAnswer(
            invocation -> {
              final Set<Urn> requested = invocation.getArgument(2);
              final Map<Urn, EntityResponse> responses = new HashMap<>();
              requested.forEach(
                  urn -> {
                    final Forms forms = existingForms.get(urn);
                    responses.put(
                        urn,
                        forms == null
                            ? entityResponse(urn, Map.of())
                            : entityResponse(urn, Map.of(FORMS_ASPECT_NAME, forms.data())));
                  });
              return responses;
            });

    return mockClient;
  }

  private static EntityResponse entityResponse(
      final Urn urn, final String aspectName, final com.linkedin.data.DataMap aspectData) {
    return entityResponse(urn, Map.of(aspectName, aspectData));
  }

  private static EntityResponse entityResponse(
      final Urn urn, final Map<String, com.linkedin.data.DataMap> aspects) {
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspects.forEach(
        (aspectName, data) ->
            aspectMap.put(
                aspectName, new EnvelopedAspect().setName(aspectName).setValue(new Aspect(data))));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(aspectMap);
  }
}
