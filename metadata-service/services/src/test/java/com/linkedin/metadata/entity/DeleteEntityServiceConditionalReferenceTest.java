package com.linkedin.metadata.entity;

import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * With conditional reference writes on, removing a reference from an aspect must not overwrite an
 * edit made to that aspect after it was read, and an entity deleted because it only exists for the
 * deleted one is kept when written to after its versions were captured. Primary storage for one
 * referencing aspect (a dataset's, or a form's formInfo) is faked in memory; a pending concurrent
 * edit lands just before the next write, as another writer's would.
 */
public class DeleteEntityServiceConditionalReferenceTest {
  private static final TagUrn DELETED_TAG = new TagUrn("deleted");
  private static final TagUrn KEPT_TAG = new TagUrn("kept");
  private static final Urn CONTAINER = UrnUtils.getUrn("urn:li:container:deleted");
  private static final Urn DATASET =
      UrnUtils.toDatasetUrn("snowflake", "conditional_reference", "PROD");
  private static final Urn DELETED_PROPERTY = UrnUtils.getUrn("urn:li:structuredProperty:deleted");
  private static final Urn KEPT_PROPERTY = UrnUtils.getUrn("urn:li:structuredProperty:kept");
  private static final Urn FORM = UrnUtils.getUrn("urn:li:form:conditionalReference");
  private static final Urn OTHER_FORM = UrnUtils.getUrn("urn:li:form:otherReference");
  private static final Urn DELETED_FORM = UrnUtils.getUrn("urn:li:form:deleted");
  private static final Urn KEPT_FORM = UrnUtils.getUrn("urn:li:form:kept");
  private static final Urn TEST = UrnUtils.getUrn("urn:li:test:formTest");
  private static final Urn OTHER_TEST = UrnUtils.getUrn("urn:li:test:otherFormTest");
  private static final long VERSION_READ = 3L;
  private static final int WRITE_LIMIT = 3;

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization()
          .withReadPreference(ReadPreference.READ);

  private EntityService<?> entityService;
  private GraphService graphService;
  private EntitySearchService searchService;
  private MetricUtils metricUtils;
  private ValidationException versionMismatch;

  private Urn storedUrn;
  private String storedAspectName;
  private RecordTemplate storedAspect;
  private long storedVersion;
  private boolean versionInSystemMetadata;
  private int pendingConcurrentEdits;
  private Runnable concurrentEdit;
  private List<MetadataChangeProposal> writes;
  private List<ReadPreference> readPreferences;

  /** Entities outside the faked one: read as given, and every write to them commits. */
  private Map<Urn, EntityResponse> otherEntities;

  private List<MetadataChangeProposal> otherWrites;

  @BeforeMethod
  public void setup() throws Exception {
    entityService = mock(EntityService.class);
    graphService = mock(GraphService.class);
    searchService = mock(EntitySearchService.class);
    metricUtils = mock(MetricUtils.class);
    storedUrn = DATASET;
    storedVersion = VERSION_READ;
    versionInSystemMetadata = true;
    pendingConcurrentEdits = 0;
    writes = new ArrayList<>();
    readPreferences = new ArrayList<>();
    otherEntities = new HashMap<>();
    otherWrites = new ArrayList<>();

    final BatchItem item = mock(BatchItem.class);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getUrn()).thenReturn(DATASET);
    when(item.getAspectName()).thenReturn(Constants.GLOBAL_TAGS_ASPECT_NAME);
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    exceptions.addException(AspectValidationException.forPrecondition(item, "version mismatch"));
    versionMismatch = new ValidationException(exceptions);

    final ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setEntities(new SearchEntityArray());
    emptyScrollResult.setNumEntities(0);
    when(searchService.structuredScroll(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            isNull(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(emptyScrollResult);

    when(entityService.getEntityV2(
            any(OperationContext.class), anyString(), any(Urn.class), anySet()))
        .thenAnswer(
            invocation -> {
              readPreferences.add(
                  invocation
                      .<OperationContext>getArgument(0)
                      .getPrimaryStorageContext()
                      .getReadPreference());
              final Urn urn = invocation.getArgument(2);
              return urn.equals(storedUrn) ? storedResponse() : otherEntities.get(urn);
            });
    when(entityService.ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false)))
        .thenAnswer(invocation -> ingest(invocation.getArgument(1)));
  }

  @Test
  public void theWriteCarriesTheVersionRead() {
    storeTags(DELETED_TAG, KEPT_TAG);

    deleteReferencesTo(DELETED_TAG, "TaggedWith", true);

    assertEquals(writes.size(), 1);
    assertEquals(ifVersionMatch(writes.get(0)), String.valueOf(VERSION_READ));
    assertEquals(storedTags(), List.of(KEPT_TAG));
  }

  @Test
  public void aConcurrentEditIsKeptAndTheReferenceIsRemovedFromIt() {
    storeTags(DELETED_TAG, KEPT_TAG);
    pendingConcurrentEdits = 1;

    deleteReferencesTo(DELETED_TAG, "TaggedWith", true);

    assertEquals(writes.size(), 2);
    assertEquals(ifVersionMatch(writes.get(0)), String.valueOf(VERSION_READ));
    assertEquals(ifVersionMatch(writes.get(1)), String.valueOf(VERSION_READ + 1));
    assertEquals(storedTags(), List.of(KEPT_TAG, concurrentTag(VERSION_READ)));
  }

  /** The aspect changes before every write: after the configured number the failure is reported. */
  @Test
  public void anAspectThatKeepsChangingIsReportedAsAFailedWrite() {
    storeTags(DELETED_TAG, KEPT_TAG);
    pendingConcurrentEdits = Integer.MAX_VALUE;

    final List<ILoggingEvent> logs =
        logsOf(() -> deleteReferencesTo(DELETED_TAG, "TaggedWith", true));

    assertEquals(writes.size(), WRITE_LIMIT);
    assertTrue(storedTags().contains(DELETED_TAG));
    assertTrue(reportsFailedUpdate(logs));
  }

  @DataProvider
  public Object[][] uncommittedUpdates() {
    return new Object[][] {
      {null}, {IngestResult.builder().urn(DATASET).sqlCommitted(false).build()}
    };
  }

  /** The write is not committed while the aspect stays at the version read: reported, once. */
  @Test(dataProvider = "uncommittedUpdates")
  public void anUncommittedUpdateAtTheVersionReadIsReportedAsAFailedWrite(
      final IngestResult result) {
    storeTags(DELETED_TAG, KEPT_TAG);
    doAnswer(
            invocation -> {
              writes.add(invocation.getArgument(1));
              return result;
            })
        .when(entityService)
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false));

    final List<ILoggingEvent> logs =
        logsOf(() -> deleteReferencesTo(DELETED_TAG, "TaggedWith", true));

    assertEquals(writes.size(), 1);
    assertTrue(storedTags().contains(DELETED_TAG));
    assertTrue(reportsFailedUpdate(logs));
  }

  @Test
  public void aSmallerWriteLimitStopsAfterThatManyWrites() {
    storeTags(DELETED_TAG, KEPT_TAG);
    pendingConcurrentEdits = Integer.MAX_VALUE;

    deleteReferencesTo(DELETED_TAG, "TaggedWith", true, 2);

    assertEquals(writes.size(), 2);
    assertTrue(storedTags().contains(DELETED_TAG));
  }

  @Test
  public void aWriteLimitBelowOneIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeleteEntityService(
                entityService, graphService, searchService, null, null, true, 0));
  }

  @Test
  public void aRequiredReferenceIsDeletedOnlyAtTheVersionRead() {
    storeContainer();

    deleteReferencesTo(CONTAINER, "IsPartOf", true);

    verify(entityService)
        .deleteAspect(
            any(OperationContext.class),
            eq(DATASET.toString()),
            eq(Constants.CONTAINER_ASPECT_NAME),
            eq(Map.of(EntityService.DELETE_CONDITION_MAX_VERSION, String.valueOf(VERSION_READ))),
            eq(true));
  }

  /**
   * A legacy aspect has no version in its system metadata, so a bound on the resolved version would
   * miss its older rows and restore them; it is deleted unbounded, as with the flag off.
   */
  @Test
  public void aRequiredReferenceOnALegacyAspectIsDeletedUnbounded() {
    storeContainer();
    versionInSystemMetadata = false;

    deleteReferencesTo(CONTAINER, "IsPartOf", true);

    verify(entityService)
        .deleteAspect(
            any(OperationContext.class),
            eq(DATASET.toString()),
            eq(Constants.CONTAINER_ASPECT_NAME),
            eq(Map.of()),
            eq(true));
  }

  @Test
  public void withTheFlagOffTheWriteIsUnconditional() {
    storeTags(DELETED_TAG, KEPT_TAG);
    pendingConcurrentEdits = 1;

    deleteReferencesTo(DELETED_TAG, "TaggedWith", false);

    assertEquals(writes.size(), 1);
    assertFalse(writes.get(0).hasHeaders());
    // Today's behaviour: the concurrent edit is overwritten.
    assertEquals(storedTags(), List.of(KEPT_TAG));
  }

  @Test
  public void withTheFlagOffARequiredReferenceDeleteIsUnbounded() {
    storeContainer();

    deleteReferencesTo(CONTAINER, "IsPartOf", false);

    verify(entityService)
        .deleteAspect(
            any(OperationContext.class),
            eq(DATASET.toString()),
            eq(Constants.CONTAINER_ASPECT_NAME),
            eq(Map.of()),
            eq(true));
  }

  @Test
  public void aConcurrentPromptEditIsKeptAndTheDeletedPromptIsStillRemoved() {
    storeFormInfo(DELETED_PROPERTY, KEPT_PROPERTY);
    searchFinds(List.of(Constants.FORM_ENTITY_NAME), FORM);
    pendingConcurrentEdits = 1;

    deleteWithoutGraphReferences(DELETED_PROPERTY, true, WRITE_LIMIT);

    assertEquals(writes.size(), 2);
    assertEquals(ifVersionMatch(writes.get(0)), String.valueOf(VERSION_READ));
    assertEquals(ifVersionMatch(writes.get(1)), String.valueOf(VERSION_READ + 1));
    assertEquals(
        storedPromptIds(), List.of(promptId(KEPT_PROPERTY), concurrentPromptId(VERSION_READ)));
    verifyNoAsyncWrites();
  }

  /**
   * A concurrent edit to an asset's forms must survive, and the deleted form must still be removed.
   */
  @Test
  public void aConcurrentFormsEditIsKeptAndTheDeletedFormIsStillRemoved() {
    storeForms(DELETED_FORM, KEPT_FORM);
    searchFinds(DeleteEntityUtils.getEntityNamesForFormDeletion(), DATASET);
    pendingConcurrentEdits = 1;

    deleteWithoutGraphReferences(DELETED_FORM, true, WRITE_LIMIT);

    assertEquals(writes.size(), 2);
    assertEquals(ifVersionMatch(writes.get(0)), String.valueOf(VERSION_READ));
    assertEquals(ifVersionMatch(writes.get(1)), String.valueOf(VERSION_READ + 1));
    assertEquals(storedFormUrns(), List.of(KEPT_FORM, concurrentForm(VERSION_READ)));
    verifyNoAsyncWrites();
  }

  /** One form keeps changing past the write limit: reported, and the next form is still cleaned. */
  @Test
  public void aFormThatKeepsChangingIsReportedAndTheNextFormIsStillCleaned() {
    storeFormInfo(DELETED_PROPERTY, KEPT_PROPERTY);
    otherEntities.put(
        OTHER_FORM,
        versionedResponse(
            OTHER_FORM,
            Constants.FORM_INFO_ASPECT_NAME,
            formInfo(DELETED_PROPERTY, KEPT_PROPERTY),
            VERSION_READ));
    searchFinds(List.of(Constants.FORM_ENTITY_NAME), FORM, OTHER_FORM);
    pendingConcurrentEdits = Integer.MAX_VALUE;

    final List<ILoggingEvent> logs =
        logsOf(() -> deleteWithoutGraphReferences(DELETED_PROPERTY, true, WRITE_LIMIT));

    assertEquals(writes.size(), WRITE_LIMIT);
    assertTrue(storedPromptIds().contains(promptId(DELETED_PROPERTY)));
    assertTrue(reportsFailedUpdate(logs));
    assertEquals(otherWrites.size(), 1);
    assertEquals(ifVersionMatch(otherWrites.get(0)), String.valueOf(VERSION_READ));
    assertEquals(promptIds(writtenFormInfo(otherWrites.get(0))), List.of(promptId(KEPT_PROPERTY)));
  }

  @Test
  public void withTheFlagOffAFormIsRewrittenAsynchronouslyWithoutAVersionCheck() {
    storeFormInfo(DELETED_PROPERTY, KEPT_PROPERTY);
    searchFinds(List.of(Constants.FORM_ENTITY_NAME), FORM);
    when(entityService.getLatestAspect(
            any(OperationContext.class), eq(FORM), eq(Constants.FORM_INFO_ASPECT_NAME)))
        .thenReturn(storedAspect);

    deleteWithoutGraphReferences(DELETED_PROPERTY, false, WRITE_LIMIT);

    assertTrue(writes.isEmpty());
    final ArgumentCaptor<MetadataChangeProposal> asyncWrite =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService)
        .ingestProposal(
            any(OperationContext.class), asyncWrite.capture(), any(AuditStamp.class), eq(true));
    assertFalse(
        asyncWrite.getValue().hasHeaders()
            && asyncWrite.getValue().getHeaders().containsKey(HTTP_HEADER_IF_VERSION_MATCH));
    assertEquals(
        promptIds(writtenFormInfo(asyncWrite.getValue())), List.of(promptId(KEPT_PROPERTY)));
  }

  @Test
  public void withTheFlagOffFormsAreRewrittenAsynchronouslyWithoutAVersionCheck() {
    storeForms(DELETED_FORM, KEPT_FORM);
    searchFinds(DeleteEntityUtils.getEntityNamesForFormDeletion(), DATASET);
    when(entityService.getLatestAspect(
            any(OperationContext.class), eq(DATASET), eq(Constants.FORMS_ASPECT_NAME)))
        .thenReturn(storedAspect);

    deleteWithoutGraphReferences(DELETED_FORM, false, WRITE_LIMIT);

    assertTrue(writes.isEmpty());
    final ArgumentCaptor<MetadataChangeProposal> asyncWrite =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService)
        .ingestProposal(
            any(OperationContext.class), asyncWrite.capture(), any(AuditStamp.class), eq(true));
    assertFalse(
        asyncWrite.getValue().hasHeaders()
            && asyncWrite.getValue().getHeaders().containsKey(HTTP_HEADER_IF_VERSION_MATCH));
    assertEquals(formUrns(writtenForms(asyncWrite.getValue())), List.of(KEPT_FORM));
  }

  @Test
  public void aTestOfTheDeletedFormIsDeletedBoundedByTheVersionsCaptured() {
    searchFinds(DeleteEntityUtils.getEntityNamesForFormDeletion(), TEST);
    final DeleteCeiling ceiling = captureCeiling(TEST);
    deletesTo(TEST, ceiling, true);

    deleteWithoutGraphReferences(DELETED_FORM, true, WRITE_LIMIT);

    verify(entityService).deleteUrn(any(OperationContext.class), eq(TEST), eq(ceiling));
    verify(entityService, never()).deleteUrn(any(OperationContext.class), eq(TEST));
  }

  /** A write after the capture keeps the test and counts an outcome; the next is still deleted. */
  @Test
  public void aTestWrittenToAfterTheCaptureIsKeptAndTheNextOneIsStillDeleted() {
    searchFinds(DeleteEntityUtils.getEntityNamesForFormDeletion(), TEST, OTHER_TEST);
    final DeleteCeiling keptCeiling = captureCeiling(TEST);
    deletesTo(TEST, keptCeiling, false);
    final DeleteCeiling otherCeiling = captureCeiling(OTHER_TEST);
    deletesTo(OTHER_TEST, otherCeiling, true);

    deleteWithoutGraphReferences(DELETED_FORM, true, WRITE_LIMIT);

    verifyCascadeOutcome("referencing_entity_kept");
    verify(entityService, never()).deleteUrn(any(OperationContext.class), eq(TEST));
    verify(entityService).deleteUrn(any(OperationContext.class), eq(OTHER_TEST), eq(otherCeiling));
  }

  /** With nothing to bound the delete by, the test is left alone and an error is counted. */
  @Test
  public void aTestWithNoCeilingIsNotDeletedAndIsCountedAsAnError() {
    searchFinds(DeleteEntityUtils.getEntityNamesForFormDeletion(), TEST);
    when(entityService.captureDeleteCeiling(any(OperationContext.class), eq(TEST)))
        .thenReturn(Optional.empty());

    deleteWithoutGraphReferences(DELETED_FORM, true, WRITE_LIMIT);

    verify(entityService, never()).deleteUrn(any(OperationContext.class), eq(TEST));
    verify(entityService, never())
        .deleteUrn(any(OperationContext.class), eq(TEST), any(DeleteCeiling.class));
    verifyCascadeError("referencing_entity_missing");
  }

  /** The cleanup's own reads must reach primary storage: a replica may not have the form yet. */
  @Test
  public void everyReadOfAFormWhileRemovingPromptsIsPinnedToPrimary() {
    storeFormInfo(DELETED_PROPERTY, KEPT_PROPERTY);
    searchFinds(List.of(Constants.FORM_ENTITY_NAME), FORM);
    pendingConcurrentEdits = 1;

    deleteWithoutGraphReferences(DELETED_PROPERTY, true, WRITE_LIMIT);

    // The first read, and the re-read after the conflict.
    assertEquals(readPreferences, List.of(ReadPreference.PRIMARY, ReadPreference.PRIMARY));
  }

  @Test
  public void aFormWithNoPromptForTheDeletedPropertyIsNotWritten() {
    storeFormInfo(KEPT_PROPERTY);
    searchFinds(List.of(Constants.FORM_ENTITY_NAME), FORM);

    deleteWithoutGraphReferences(DELETED_PROPERTY, true, WRITE_LIMIT);

    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            anyBoolean());
  }

  @Test
  public void withTheFlagOffATestOfTheDeletedFormIsDeletedUnbounded() {
    searchFinds(DeleteEntityUtils.getEntityNamesForFormDeletion(), TEST);

    deleteWithoutGraphReferences(DELETED_FORM, false, WRITE_LIMIT);

    verify(entityService).deleteUrn(any(OperationContext.class), eq(TEST));
    verify(entityService, never()).captureDeleteCeiling(any(OperationContext.class), any());
  }

  private void searchFinds(final List<String> entityNames, final Urn... found) {
    final SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : found) {
      entities.add(new SearchEntity().setEntity(urn));
    }
    final ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(entities);
    scrollResult.setNumEntities(found.length);
    when(searchService.structuredScroll(
            any(OperationContext.class),
            eq(entityNames),
            anyString(),
            any(Filter.class),
            isNull(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(scrollResult);
  }

  private void deleteWithoutGraphReferences(
      final Urn deleted, final boolean conditional, final int writeLimit) {
    when(graphService.scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", deleted.toString())),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of()),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            eq("5m"),
            eq(1000),
            nullable(Long.class),
            nullable(Long.class)))
        .thenReturn(
            RelatedEntitiesScrollResult.builder()
                .numResults(0)
                .pageSize(0)
                .scrollId(null)
                .entities(ImmutableList.of())
                .build());
    new DeleteEntityService(
            entityService, graphService, searchService, null, metricUtils, conditional, writeLimit)
        .deleteReferencesTo(opContext, deleted, false);
  }

  private DeleteCeiling captureCeiling(final Urn urn) {
    final DeleteCeiling ceiling = new DeleteCeiling(Map.of(urn.getEntityType() + "Key", 1L), 1L);
    when(entityService.captureDeleteCeiling(any(OperationContext.class), eq(urn)))
        .thenReturn(Optional.of(ceiling));
    return ceiling;
  }

  /** {@code keyDeleted} false: the entity was written to after the capture and is kept. */
  private void deletesTo(final Urn urn, final DeleteCeiling ceiling, final boolean keyDeleted) {
    final RollbackResult result =
        new RollbackResult(
            urn,
            urn.getEntityType(),
            urn.getEntityType() + "Key",
            null,
            null,
            null,
            null,
            ChangeType.DELETE,
            keyDeleted,
            0);
    when(entityService.deleteUrn(any(OperationContext.class), eq(urn), eq(ceiling)))
        .thenReturn(new RollbackRunResult(List.of(), keyDeleted ? 1 : 0, List.of(result)));
  }

  private void verifyCascadeError(final String errorType) {
    verify(metricUtils)
        .incrementMicrometer(
            "datahub.cascade.errors",
            1,
            "operation_type",
            "deleteReferencesTo",
            "trigger_urn_type",
            DELETED_FORM.getEntityType(),
            "error_type",
            errorType);
  }

  private void verifyCascadeOutcome(final String outcome) {
    verify(metricUtils)
        .incrementMicrometer(
            "datahub.cascade.outcomes",
            1,
            "operation_type",
            "deleteReferencesTo",
            "trigger_urn_type",
            DELETED_FORM.getEntityType(),
            "outcome",
            outcome);
  }

  private void verifyNoAsyncWrites() {
    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(true));
  }

  private static FormInfo formInfo(final Urn... properties) {
    final FormPromptArray prompts = new FormPromptArray();
    for (Urn property : properties) {
      prompts.add(prompt(promptId(property), property));
    }
    return new FormInfo().setName("form").setPrompts(prompts);
  }

  private static FormPrompt prompt(final String id, final Urn property) {
    return new FormPrompt()
        .setId(id)
        .setTitle(id)
        .setType(FormPromptType.STRUCTURED_PROPERTY)
        .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(property));
  }

  private static String promptId(final Urn property) {
    return "prompt-" + property.getId();
  }

  private static String concurrentPromptId(final long version) {
    return "concurrent" + version;
  }

  private List<String> storedPromptIds() {
    return promptIds(new FormInfo(storedAspect.data()));
  }

  private static List<String> promptIds(final FormInfo formInfo) {
    return formInfo.getPrompts().stream().map(FormPrompt::getId).collect(Collectors.toList());
  }

  private static FormInfo writtenFormInfo(final MetadataChangeProposal proposal) {
    return GenericRecordUtils.deserializeAspect(
        proposal.getAspect().getValue(), proposal.getAspect().getContentType(), FormInfo.class);
  }

  private static Forms formsAspect(final Urn... forms) {
    final FormAssociationArray incomplete = new FormAssociationArray();
    for (Urn form : forms) {
      incomplete.add(formAssociation(form));
    }
    return new Forms()
        .setIncompleteForms(incomplete)
        .setCompletedForms(new FormAssociationArray())
        .setVerifications(new FormVerificationAssociationArray());
  }

  private static FormAssociation formAssociation(final Urn form) {
    return new FormAssociation().setUrn(form);
  }

  private static Urn concurrentForm(final long version) {
    return UrnUtils.getUrn("urn:li:form:concurrent" + version);
  }

  private List<Urn> storedFormUrns() {
    return formUrns(new Forms(storedAspect.data()));
  }

  private static List<Urn> formUrns(final Forms forms) {
    return forms.getIncompleteForms().stream()
        .map(FormAssociation::getUrn)
        .collect(Collectors.toList());
  }

  private static Forms writtenForms(final MetadataChangeProposal proposal) {
    return GenericRecordUtils.deserializeAspect(
        proposal.getAspect().getValue(), proposal.getAspect().getContentType(), Forms.class);
  }

  private static EntityResponse versionedResponse(
      final Urn urn, final String aspectName, final RecordTemplate aspect, final long version) {
    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(aspectName)
            .setValue(new Aspect(aspect.data()))
            .setVersion(0L)
            .setSystemMetadata(new SystemMetadata().setVersion(String.valueOf(version)));
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urn.getEntityType());
    response.setAspects(new EnvelopedAspectMap(Map.of(aspectName, envelopedAspect)));
    return response;
  }

  private void deleteReferencesTo(
      final Urn deleted, final String relationshipType, final boolean conditional) {
    deleteReferencesTo(deleted, relationshipType, conditional, WRITE_LIMIT);
  }

  private void deleteReferencesTo(
      final Urn deleted,
      final String relationshipType,
      final boolean conditional,
      final int writeLimit) {
    when(graphService.scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", deleted.toString())),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of()),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            eq("5m"),
            eq(1000),
            nullable(Long.class),
            nullable(Long.class)))
        .thenReturn(
            RelatedEntitiesScrollResult.builder()
                .numResults(1)
                .pageSize(1)
                .scrollId(null)
                .entities(
                    ImmutableList.of(
                        new RelatedEntities(
                            relationshipType,
                            DATASET.toString(),
                            deleted.toString(),
                            RelationshipDirection.INCOMING,
                            null)))
                .build());
    new DeleteEntityService(
            entityService, graphService, searchService, null, null, conditional, writeLimit)
        .deleteReferencesTo(opContext, deleted, false);
  }

  private static List<ILoggingEvent> logsOf(final Runnable action) {
    final Logger logger = (Logger) LoggerFactory.getLogger(DeleteEntityService.class);
    final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    logs.start();
    logger.addAppender(logs);
    try {
      action.run();
    } finally {
      logger.detachAppender(logs);
    }
    return logs.list;
  }

  private static boolean reportsFailedUpdate(final List<ILoggingEvent> logs) {
    return logs.stream()
        .anyMatch(
            event ->
                event.getLevel() == Level.WARN
                    && event.getFormattedMessage().contains("MCP_PROCESSOR_FAILED"));
  }

  private void storeTags(final TagUrn... tags) {
    storedAspectName = Constants.GLOBAL_TAGS_ASPECT_NAME;
    final TagAssociationArray associations = new TagAssociationArray();
    for (TagUrn tag : tags) {
      associations.add(new TagAssociation().setTag(tag));
    }
    storedAspect = new GlobalTags().setTags(associations);
    concurrentEdit =
        () -> {
          final List<TagUrn> edited = new ArrayList<>(storedTags());
          edited.add(concurrentTag(storedVersion));
          storeTags(edited.toArray(new TagUrn[0]));
        };
  }

  /**
   * A dataset whose forms names the given forms; a concurrent edit adds another form assignment.
   */
  private void storeForms(final Urn... forms) {
    storedUrn = DATASET;
    storedAspectName = Constants.FORMS_ASPECT_NAME;
    storedAspect = formsAspect(forms);
    concurrentEdit =
        () -> {
          final Forms edited = new Forms(storedAspect.data());
          final List<FormAssociation> incomplete = new ArrayList<>(edited.getIncompleteForms());
          incomplete.add(formAssociation(concurrentForm(storedVersion)));
          storedAspect =
              new Forms()
                  .setIncompleteForms(new FormAssociationArray(incomplete))
                  .setCompletedForms(edited.getCompletedForms())
                  .setVerifications(edited.getVerifications());
        };
  }

  /** A form whose formInfo has one prompt per property; a concurrent edit adds another prompt. */
  private void storeFormInfo(final Urn... properties) {
    storedUrn = FORM;
    storedAspectName = Constants.FORM_INFO_ASPECT_NAME;
    storedAspect = formInfo(properties);
    concurrentEdit =
        () -> {
          final FormInfo edited = new FormInfo(storedAspect.data());
          final List<FormPrompt> prompts = new ArrayList<>(edited.getPrompts());
          prompts.add(prompt(concurrentPromptId(storedVersion), KEPT_PROPERTY));
          storedAspect = new FormInfo().setName("form").setPrompts(new FormPromptArray(prompts));
        };
  }

  private void storeContainer() {
    storedAspectName = Constants.CONTAINER_ASPECT_NAME;
    storedAspect = new Container().setContainer(CONTAINER);
    when(entityService.deleteAspect(
            any(OperationContext.class),
            eq(DATASET.toString()),
            eq(Constants.CONTAINER_ASPECT_NAME),
            anyMap(),
            anyBoolean()))
        .thenReturn(
            Optional.of(
                new RollbackResult(
                    DATASET,
                    Constants.DATASET_ENTITY_NAME,
                    Constants.CONTAINER_ASPECT_NAME,
                    storedAspect,
                    null,
                    null,
                    null,
                    ChangeType.DELETE,
                    false,
                    1)));
  }

  private List<TagUrn> storedTags() {
    final GlobalTags globalTags = (GlobalTags) storedAspect;
    return globalTags.getTags().stream().map(TagAssociation::getTag).collect(Collectors.toList());
  }

  private static TagUrn concurrentTag(final long version) {
    return new TagUrn("concurrent" + version);
  }

  private static String ifVersionMatch(final MetadataChangeProposal proposal) {
    return proposal.getHeaders().get(HTTP_HEADER_IF_VERSION_MATCH);
  }

  private EntityResponse storedResponse() {
    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(storedAspectName)
            .setValue(new Aspect(storedAspect.data()))
            .setVersion(0L)
            .setSystemMetadata(
                versionInSystemMetadata
                    ? new SystemMetadata().setVersion(String.valueOf(storedVersion))
                    : new SystemMetadata());
    final EntityResponse response = new EntityResponse();
    response.setUrn(storedUrn);
    response.setEntityName(storedUrn.getEntityType());
    response.setAspects(new EnvelopedAspectMap(Map.of(storedAspectName, envelopedAspect)));
    return response;
  }

  /** Primary storage with the version precondition: a concurrent edit may land first. */
  private IngestResult ingest(final MetadataChangeProposal proposal) {
    if (!proposal.getEntityUrn().equals(storedUrn)) {
      otherWrites.add(proposal);
      return IngestResult.builder().urn(proposal.getEntityUrn()).sqlCommitted(true).build();
    }
    writes.add(proposal);
    if (pendingConcurrentEdits > 0) {
      pendingConcurrentEdits--;
      concurrentEdit.run();
      storedVersion++;
    }
    if (proposal.hasHeaders() && !String.valueOf(storedVersion).equals(ifVersionMatch(proposal))) {
      throw versionMismatch;
    }
    storedAspect =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            storedAspect.getClass());
    storedVersion++;
    return IngestResult.builder().urn(storedUrn).sqlCommitted(true).build();
  }
}
