package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.DocumentChange;
import com.linkedin.datahub.graphql.generated.DocumentChangeType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for Document.changeHistory field. Fetches change history for a document using the
 * Timeline Service and converts it to a simple, document-native format.
 */
@Slf4j
@RequiredArgsConstructor
public class DocumentChangeHistoryResolver
    implements DataFetcher<CompletableFuture<List<DocumentChange>>> {

  private final TimelineService _timelineService;
  private static final long DEFAULT_LOOKBACK_MILLIS = 30L * 24 * 60 * 60 * 1000; // 30 days
  private static final int DEFAULT_LIMIT = 50;

  @Override
  public CompletableFuture<List<DocumentChange>> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Document source = environment.getSource();
    final Urn documentUrn = UrnUtils.getUrn(source.getUrn());

    // Parse arguments
    final Long startTimeMillis = environment.getArgument("startTimeMillis");
    final Long endTimeMillis = environment.getArgument("endTimeMillis");
    final Integer limit = environment.getArgument("limit");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Calculate time range
            long endTime = endTimeMillis != null ? endTimeMillis : System.currentTimeMillis();
            long startTime =
                startTimeMillis != null ? startTimeMillis : (endTime - DEFAULT_LOOKBACK_MILLIS);
            int maxResults = limit != null ? limit : DEFAULT_LIMIT;

            // Fetch all relevant change categories for documents
            Set<ChangeCategory> categories = getAllDocumentChangeCategories();

            // Get timeline from TimelineService
            List<ChangeTransaction> transactions =
                _timelineService.getTimeline(
                    documentUrn,
                    categories,
                    startTime,
                    endTime,
                    null, // startVersionStamp
                    null, // endVersionStamp
                    false); // rawDiffsRequested

            // Convert to document-native format and flatten
            List<DocumentChange> changes = new ArrayList<>();
            Set<String> seenChanges = new HashSet<>(); // For deduplication

            for (ChangeTransaction transaction : transactions) {
              if (transaction.getChangeEvents() != null) {
                for (ChangeEvent event : transaction.getChangeEvents()) {
                  DocumentChange change = convertToDocumentChange(event);
                  if (change != null) {
                    // Create a unique key for deduplication: timestamp + changeType + description
                    String changeKey =
                        String.format(
                            "%s_%s_%s",
                            change.getTimestamp(), change.getChangeType(), change.getDescription());

                    // Only add if we haven't seen this exact change before
                    if (!seenChanges.contains(changeKey)) {
                      seenChanges.add(changeKey);
                      changes.add(change);
                    }
                  }
                }
              }
            }

            // Sort by timestamp descending (most recent first) and limit
            // Handle null timestamps by treating them as 0
            changes.sort(
                (a, b) -> {
                  Long aTime = a.getTimestamp() != null ? a.getTimestamp() : 0L;
                  Long bTime = b.getTimestamp() != null ? b.getTimestamp() : 0L;
                  return Long.compare(bTime, aTime);
                });
            if (changes.size() > maxResults) {
              changes = changes.subList(0, maxResults);
            }

            return changes;
          } catch (Exception e) {
            log.error(
                "Failed to fetch change history for document {}: {}",
                documentUrn,
                e.getMessage(),
                e);
            throw new RuntimeException("Failed to fetch change history: " + e.getMessage(), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Get all change categories relevant to documents. This includes documentation changes, lifecycle
   * events, parent changes, and related entity changes.
   */
  private Set<ChangeCategory> getAllDocumentChangeCategories() {
    Set<ChangeCategory> categories = new HashSet<>();
    categories.add(ChangeCategory.DOCUMENTATION); // content/title changes
    categories.add(ChangeCategory.LIFECYCLE); // creation, state changes
    categories.add(ChangeCategory.PARENT); // parent document changes
    categories.add(ChangeCategory.RELATED_ENTITIES); // related assets/documents
    return categories;
  }

  /**
   * Convert a Timeline ChangeEvent to a document-native DocumentChange. This abstracts away the
   * Timeline Service implementation details and provides a clean, simple interface for document
   * changes.
   */
  @Nullable
  private DocumentChange convertToDocumentChange(ChangeEvent event) {
    if (event == null) {
      return null;
    }

    DocumentChange change = new DocumentChange();

    // Map change type
    DocumentChangeType changeType = mapToDocumentChangeType(event);
    if (changeType == null) {
      return null; // Skip unmapped events
    }
    change.setChangeType(changeType);

    // Set description
    change.setDescription(
        event.getDescription() != null ? event.getDescription() : "Change occurred");

    // Set timestamp
    change.setTimestamp(
        event.getAuditStamp() != null
            ? event.getAuditStamp().getTime()
            : System.currentTimeMillis());

    // Set actor (optional)
    if (event.getAuditStamp() != null && event.getAuditStamp().hasActor()) {
      CorpUser actor = new CorpUser();
      actor.setUrn(event.getAuditStamp().getActor().toString());
      actor.setType(EntityType.CORP_USER);
      change.setActor(actor);
    }

    // Set details (optional parameters)
    if (event.getParameters() != null && !event.getParameters().isEmpty()) {
      List<StringMapEntry> details = new ArrayList<>();
      for (Map.Entry<String, Object> entry : event.getParameters().entrySet()) {
        StringMapEntry mapEntry = new StringMapEntry();
        mapEntry.setKey(entry.getKey());
        mapEntry.setValue(entry.getValue() != null ? entry.getValue().toString() : "");
        details.add(mapEntry);
      }
      change.setDetails(details);
    }

    return change;
  }

  /**
   * Map Timeline ChangeEvent to document-specific DocumentChangeType. This provides a clean
   * abstraction layer that can be swapped out for event-based tracking in the future.
   */
  @Nullable
  private DocumentChangeType mapToDocumentChangeType(ChangeEvent event) {
    ChangeCategory category = event.getCategory();
    ChangeOperation operation = event.getOperation();

    if (category == null || operation == null) {
      return null;
    }

    // Creation events
    if (operation == ChangeOperation.CREATE) {
      return DocumentChangeType.CREATED;
    }

    // Map based on category and description patterns
    switch (category) {
      case DOCUMENTATION:
        // Differentiate between title and text content changes using parameters
        if (event.getParameters() != null) {
          if (event.getParameters().containsKey("oldTitle")
              || event.getParameters().containsKey("newTitle")) {
            return DocumentChangeType.TITLE_CHANGED;
          }
          if (event.getParameters().containsKey("oldContent")
              || event.getParameters().containsKey("newContent")) {
            return DocumentChangeType.TEXT_CHANGED;
          }
        }
        // Fallback: check description for backward compatibility
        if (event.getDescription() != null
            && event.getDescription().toLowerCase().contains("title")) {
          return DocumentChangeType.TITLE_CHANGED;
        }
        return DocumentChangeType.TEXT_CHANGED;

      case LIFECYCLE:
        // State changes or deletion
        if (operation == ChangeOperation.REMOVE) {
          return DocumentChangeType.DELETED;
        }
        if (event.getDescription() != null && event.getDescription().contains("state")) {
          return DocumentChangeType.STATE_CHANGED;
        }
        return DocumentChangeType.CREATED;

      case PARENT:
        // Parent relationship changes
        return DocumentChangeType.PARENT_CHANGED;

      case RELATED_ENTITIES:
        // Related entity changes - differentiate between assets and documents
        if (event.getDescription() != null) {
          String desc = event.getDescription().toLowerCase();
          if (desc.contains("asset")) {
            return DocumentChangeType.RELATED_ASSETS_CHANGED;
          } else if (desc.contains("document")) {
            return DocumentChangeType.RELATED_DOCUMENTS_CHANGED;
          }
        }
        // Default to related documents if description is unclear
        return DocumentChangeType.RELATED_DOCUMENTS_CHANGED;

      default:
        return null;
    }
  }
}
