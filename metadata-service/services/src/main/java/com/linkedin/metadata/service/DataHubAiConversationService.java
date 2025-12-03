package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.conversation.DataHubAiConversationActor;
import com.linkedin.conversation.DataHubAiConversationActorType;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.conversation.DataHubAiConversationMessage;
import com.linkedin.conversation.DataHubAiConversationMessageArray;
import com.linkedin.conversation.DataHubAiConversationMessageContent;
import com.linkedin.conversation.DataHubAiConversationMessageType;
import com.linkedin.conversation.DataHubAiConversationOriginType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.DataHubAiConversationKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing DataHub Agent Conversations.
 *
 * <p>This service handles CRUD operations for conversations, including: - Creating new
 * conversations - Listing conversations for a user - Getting conversation details - Adding messages
 * to conversations - Deleting conversations
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class DataHubAiConversationService {

  private static final String ENTITY_NAME = "dataHubAiConversation";
  private static final String INFO_ASPECT_NAME = "dataHubAiConversationInfo";

  private final SystemEntityClient entityClient;

  public DataHubAiConversationService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Creates a new conversation.
   *
   * @param opContext the operation context
   * @param title the conversation title (optional)
   * @param actorUrn the URN of the user creating the conversation
   * @return the URN of the created conversation
   * @throws Exception if creation fails
   */
  @Nonnull
  public Urn createConversation(
      @Nonnull OperationContext opContext,
      @Nullable String title,
      @Nonnull Urn actorUrn,
      @Nonnull DataHubAiConversationOriginType originType)
      throws Exception {

    // Generate conversation URN
    final String conversationId = String.valueOf(System.currentTimeMillis());
    final Urn conversationUrn =
        Urn.createFromString(String.format("urn:li:%s:%s", ENTITY_NAME, conversationId));

    // Create conversation key
    final DataHubAiConversationKey conversationKey = new DataHubAiConversationKey();
    conversationKey.setId(conversationId);

    // Create conversation info
    final DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();
    conversationInfo.setMessages(new DataHubAiConversationMessageArray());

    // Set title if provided
    if (title != null && !title.trim().isEmpty()) {
      conversationInfo.setTitle(title);
    }

    // Set created audit stamp
    final AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(actorUrn);
    conversationInfo.setCreated(created);

    // Set origin type
    conversationInfo.setOriginType(originType);

    // Emit the conversation using MetadataChangeProposal
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(conversationUrn);
    mcp.setEntityType(ENTITY_NAME);
    mcp.setAspectName(INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(conversationInfo));

    entityClient.batchIngestProposals(opContext, List.of(mcp), false);

    log.info("Created conversation {} for user {}", conversationUrn, actorUrn);
    return conversationUrn;
  }

  /**
   * Gets a conversation by URN.
   *
   * @param opContext the operation context
   * @param conversationUrn the conversation URN
   * @return the conversation info, or null if not found
   * @throws Exception if retrieval fails
   */
  @Nullable
  public DataHubAiConversationInfo getConversation(
      @Nonnull OperationContext opContext, @Nonnull Urn conversationUrn) throws Exception {

    final EntityResponse response =
        entityClient.getV2(opContext, ENTITY_NAME, conversationUrn, Set.of(INFO_ASPECT_NAME));

    if (response == null || !response.hasAspects()) {
      return null;
    }

    final EnvelopedAspect envelopedAspect = response.getAspects().get(INFO_ASPECT_NAME);
    if (envelopedAspect == null) {
      return null;
    }

    return new DataHubAiConversationInfo(envelopedAspect.getValue().data());
  }

  /**
   * Lists conversations for a user with total count.
   *
   * @param opContext the operation context
   * @param actorUrn the user URN
   * @param count the number of conversations to return
   * @param start the starting offset for pagination
   * @return a conversation list result containing both conversations and total count
   * @throws Exception if listing fails
   */
  @Nonnull
  public ConversationListResult listConversations(
      @Nonnull OperationContext opContext, @Nonnull Urn actorUrn, int count, int start)
      throws Exception {

    // Search for conversations created by the user
    final SortCriterion sortCriterion =
        new SortCriterion().setField("createdAt").setOrder(SortOrder.DESCENDING);
    final SearchResult searchResult =
        entityClient.search(
            opContext,
            ENTITY_NAME,
            "*",
            createCreatorFilter(actorUrn),
            ImmutableList.of(sortCriterion),
            start,
            count);

    if (searchResult == null
        || searchResult.getEntities() == null
        || searchResult.getEntities().isEmpty()) {
      return new ConversationListResult(
          Collections.emptyList(), searchResult != null ? searchResult.getNumEntities() : 0);
    }

    // Get conversation details
    final Set<Urn> conversationUrns =
        searchResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toSet());

    final Map<Urn, EntityResponse> conversationDetails =
        entityClient.batchGetV2(opContext, ENTITY_NAME, conversationUrns, Set.of(INFO_ASPECT_NAME));

    // Map to result objects
    final List<ConversationResult> results = new ArrayList<>();
    for (final Urn urn : conversationUrns) {
      final EntityResponse entityResponse = conversationDetails.get(urn);
      if (entityResponse != null && entityResponse.hasAspects()) {
        final EnvelopedAspect envelopedAspect = entityResponse.getAspects().get(INFO_ASPECT_NAME);
        if (envelopedAspect != null) {
          final DataHubAiConversationInfo conversationInfo =
              new DataHubAiConversationInfo(envelopedAspect.getValue().data());
          results.add(new ConversationResult(urn, conversationInfo));
        }
      }
    }

    return new ConversationListResult(results, searchResult.getNumEntities());
  }

  /**
   * Adds a message to a conversation.
   *
   * @param opContext the operation context
   * @param conversationUrn the conversation URN
   * @param actorUrn the actor adding the message
   * @param actorType the type of actor (USER or AGENT)
   * @param messageType the type of message
   * @param messageText the message text
   * @return the updated conversation info
   * @throws Exception if adding message fails
   */
  @Nonnull
  public DataHubAiConversationInfo addMessage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn conversationUrn,
      @Nonnull Urn actorUrn,
      @Nonnull DataHubAiConversationActorType actorType,
      @Nonnull DataHubAiConversationMessageType messageType,
      @Nonnull String messageText,
      @Nullable String agentName)
      throws Exception {

    // Get existing conversation
    final DataHubAiConversationInfo conversationInfo = getConversation(opContext, conversationUrn);
    if (conversationInfo == null) {
      throw new IllegalArgumentException("Conversation not found: " + conversationUrn);
    }

    // Create message
    final DataHubAiConversationMessage message = new DataHubAiConversationMessage();
    message.setType(messageType);
    message.setTime(System.currentTimeMillis());

    // Set actor
    final DataHubAiConversationActor actor = new DataHubAiConversationActor();
    actor.setType(actorType);
    actor.setActor(actorUrn);
    message.setActor(actor);

    // Set content
    final DataHubAiConversationMessageContent content = new DataHubAiConversationMessageContent();
    content.setText(messageText);
    message.setContent(content);

    // Set agent name if provided
    if (agentName != null) {
      message.setAgentName(agentName);
    }

    // Add message to conversation
    if (!conversationInfo.hasMessages()) {
      conversationInfo.setMessages(new DataHubAiConversationMessageArray());
    }
    conversationInfo.getMessages().add(message);

    // Generate title from first user message if not set
    if (actorType == DataHubAiConversationActorType.USER
        && messageType == DataHubAiConversationMessageType.TEXT
        && (!conversationInfo.hasTitle()
            || conversationInfo.getTitle() == null
            || conversationInfo.getTitle().trim().isEmpty())
        && conversationInfo.getMessages().size() == 1) {
      final String generatedTitle = generateTitleFromMessage(messageText);
      conversationInfo.setTitle(generatedTitle);
      log.info("Generated title for conversation {}: {}", conversationUrn, generatedTitle);
    }

    // Update conversation
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(conversationUrn);
    mcp.setEntityType(ENTITY_NAME);
    mcp.setAspectName(INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(conversationInfo));

    entityClient.batchIngestProposals(opContext, List.of(mcp), false);

    log.info("Added {} message to conversation {} by {}", messageType, conversationUrn, actorUrn);
    return conversationInfo;
  }

  /**
   * Deletes a conversation.
   *
   * @param opContext the operation context
   * @param conversationUrn the conversation URN to delete
   * @throws Exception if deletion fails
   */
  public void deleteConversation(@Nonnull OperationContext opContext, @Nonnull Urn conversationUrn)
      throws Exception {

    entityClient.deleteEntity(opContext, conversationUrn);
    log.info("Deleted conversation {}", conversationUrn);
  }

  /**
   * Checks if a user can access a conversation (i.e., they created it).
   *
   * @param opContext the operation context
   * @param conversationUrn the conversation URN
   * @param actorUrn the user URN
   * @return true if the user can access the conversation
   * @throws Exception if check fails
   */
  public boolean canAccessConversation(
      @Nonnull OperationContext opContext, @Nonnull Urn conversationUrn, @Nonnull Urn actorUrn)
      throws Exception {

    if (opContext.isSystemAuth()) {
      return true;
    }

    final DataHubAiConversationInfo conversationInfo = getConversation(opContext, conversationUrn);
    if (conversationInfo == null) {
      return false;
    }

    // Check if user is the creator
    if (conversationInfo.hasCreated() && conversationInfo.getCreated().hasActor()) {
      return conversationInfo.getCreated().getActor().equals(actorUrn);
    }

    return false;
  }

  /**
   * Generates a title from a message text.
   *
   * @param messageText the message text
   * @return a title generated from the first ~50 characters of the message
   */
  private String generateTitleFromMessage(@Nonnull String messageText) {
    if (messageText == null || messageText.trim().isEmpty()) {
      return "Untitled Conversation";
    }

    // Remove newlines and extra whitespace
    String cleaned = messageText.replaceAll("\\s+", " ").trim();

    // Take first 50 characters
    if (cleaned.length() > 50) {
      return cleaned.substring(0, 47) + "...";
    }

    return cleaned;
  }

  /** Creates a filter for searching conversations by creator. */
  private Filter createCreatorFilter(Urn creatorUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion criterion =
        CriterionUtils.buildCriterion("creator", Condition.EQUAL, creatorUrn.toString());

    andCriterion.add(criterion);
    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  /** Result object containing conversation URN and info. */
  public static class ConversationResult {
    @Nonnull private final Urn urn;
    @Nonnull private final DataHubAiConversationInfo info;

    public ConversationResult(@Nonnull Urn urn, @Nonnull DataHubAiConversationInfo info) {
      this.urn = urn;
      this.info = info;
    }

    @Nonnull
    public Urn getUrn() {
      return urn;
    }

    @Nonnull
    public DataHubAiConversationInfo getInfo() {
      return info;
    }
  }

  /** Result object containing conversation list and total count. */
  public static class ConversationListResult {
    @Nonnull private final List<ConversationResult> conversations;
    private final int totalCount;

    public ConversationListResult(@Nonnull List<ConversationResult> conversations, int totalCount) {
      this.conversations = conversations;
      this.totalCount = totalCount;
    }

    @Nonnull
    public List<ConversationResult> getConversations() {
      return conversations;
    }

    public int getTotalCount() {
      return totalCount;
    }
  }
}
