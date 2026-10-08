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
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * With conditional reference writes on, removing a reference from an aspect must not overwrite an
 * edit made to that aspect after it was read. Primary storage for one referencing dataset aspect is
 * faked in memory; a pending concurrent edit lands just before the next write, as another writer's
 * would.
 */
public class DeleteEntityServiceConditionalReferenceTest {
  private static final TagUrn DELETED_TAG = new TagUrn("deleted");
  private static final TagUrn KEPT_TAG = new TagUrn("kept");
  private static final Urn CONTAINER = UrnUtils.getUrn("urn:li:container:deleted");
  private static final Urn DATASET =
      UrnUtils.toDatasetUrn("snowflake", "conditional_reference", "PROD");
  private static final long VERSION_READ = 3L;
  private static final int WRITE_LIMIT = 3;

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> entityService;
  private GraphService graphService;
  private EntitySearchService searchService;
  private ValidationException versionMismatch;

  private String storedAspectName;
  private RecordTemplate storedAspect;
  private long storedVersion;
  private boolean versionInSystemMetadata;
  private int pendingConcurrentEdits;
  private List<MetadataChangeProposal> writes;

  @BeforeMethod
  public void setup() throws Exception {
    entityService = mock(EntityService.class);
    graphService = mock(GraphService.class);
    searchService = mock(EntitySearchService.class);
    storedVersion = VERSION_READ;
    versionInSystemMetadata = true;
    pendingConcurrentEdits = 0;
    writes = new ArrayList<>();

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
            any(OperationContext.class), eq(DATASET.getEntityType()), eq(DATASET), anySet()))
        .thenAnswer(invocation -> storedResponse());
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
    response.setUrn(DATASET);
    response.setEntityName(DATASET.getEntityType());
    response.setAspects(new EnvelopedAspectMap(Map.of(storedAspectName, envelopedAspect)));
    return response;
  }

  /** Primary storage with the version precondition: a concurrent edit may land first. */
  private IngestResult ingest(final MetadataChangeProposal proposal) {
    writes.add(proposal);
    if (pendingConcurrentEdits > 0) {
      pendingConcurrentEdits--;
      final List<TagUrn> tags = new ArrayList<>(storedTags());
      tags.add(concurrentTag(storedVersion));
      storeTags(tags.toArray(new TagUrn[0]));
      storedVersion++;
    }
    if (proposal.hasHeaders() && !String.valueOf(storedVersion).equals(ifVersionMatch(proposal))) {
      throw versionMismatch;
    }
    storedAspect =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            GlobalTags.class);
    storedVersion++;
    return IngestResult.builder().urn(DATASET).sqlCommitted(true).build();
  }
}
