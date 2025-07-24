package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.module.DataHubPageModuleParams;
import com.linkedin.module.DataHubPageModuleProperties;
import com.linkedin.module.DataHubPageModuleType;
import com.linkedin.module.DataHubPageModuleVisibility;
import com.linkedin.module.LinkModuleParams;
import com.linkedin.module.PageModuleScope;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostInfo;
import com.linkedin.post.PostType;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRow;
import com.linkedin.template.DataHubPageTemplateRowArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MigrateHomePageLinksStep extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "migrate-homepage-links";
  private static final String DEFAULT_HOME_PAGE_TEMPLATE_URN =
      "urn:li:dataHubPageTemplate:home_default_1";
  private static final Integer BATCH_SIZE = 1000;

  private final EntitySearchService _entitySearchService;

  public MigrateHomePageLinksStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService) {
    super(entityService, VERSION, UPGRADE_ID);
    this._entitySearchService = entitySearchService;
  }

  @Override
  public void upgrade(@Nonnull OperationContext systemOperationContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    log.info("Starting migration of home page links to page modules...");

    // Step 1: Fetch existing homepage links
    List<Urn> postUrns = fetchPostUrns(systemOperationContext);
    log.info("Found {} home page announcement posts to migrate", postUrns.size());

    if (postUrns.isEmpty()) {
      log.info("No home page announcement posts found. Skipping migration.");
      return;
    }

    // Step 2: Fetch the default home page template
    Urn templateUrn = UrnUtils.getUrn(DEFAULT_HOME_PAGE_TEMPLATE_URN);
    DataHubPageTemplateProperties templateProperties =
        fetchPageTemplateProperties(systemOperationContext, templateUrn);

    if (templateProperties == null) {
      log.warn("Default home page template not found. Skipping migration.");
      return;
    }

    // Step 3: Convert posts to page modules and create them
    List<Urn> moduleUrns = convertPostsToPageModules(systemOperationContext, postUrns, auditStamp);
    log.info("Created {} page modules from home page posts", moduleUrns.size());

    if (moduleUrns.size() == 0) {
      return;
    }

    // Step 4: Update the home page template with the new modules
    updateHomePageTemplate(
        systemOperationContext, templateUrn, templateProperties, moduleUrns, auditStamp);
    log.info("Updated home page template with migrated modules");

    log.info("Home page links migration completed successfully");
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private List<Urn> fetchPostUrns(@Nonnull OperationContext systemOperationContext) {
    // There's no filtering for type, so we just need to get all posts
    SearchResult searchResult =
        _entitySearchService.search(
            systemOperationContext.withSearchFlags(
                flags ->
                    flags.setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true)),
            List.of(Constants.POST_ENTITY_NAME),
            "*",
            null,
            null,
            0,
            BATCH_SIZE);

    return searchResult.getEntities().stream()
        .map(SearchEntity::getEntity)
        .collect(Collectors.toList());
  }

  private DataHubPageTemplateProperties fetchPageTemplateProperties(
      @Nonnull OperationContext systemOperationContext, @Nonnull Urn templateUrn) {
    try {
      EntityResponse response =
          entityService.getEntityV2(
              systemOperationContext,
              Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME,
              templateUrn,
              Collections.singleton(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME));

      if (response != null
          && response
              .getAspects()
              .containsKey(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME)) {
        EnvelopedAspect aspect =
            response.getAspects().get(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME);
        return new DataHubPageTemplateProperties(aspect.getValue().data());
      }
    } catch (Exception e) {
      log.error("Failed to fetch page template properties for urn: {}", templateUrn, e);
    }
    return null;
  }

  private List<Urn> convertPostsToPageModules(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull List<Urn> postUrns,
      @Nonnull AuditStamp auditStamp)
      throws Exception {

    List<Urn> moduleUrns = new ArrayList<>();

    // Fetch all post info aspects
    Map<Urn, EntityResponse> postResponses =
        entityService.getEntitiesV2(
            systemOperationContext,
            Constants.POST_ENTITY_NAME,
            postUrns.stream().collect(Collectors.toSet()),
            Collections.singleton(Constants.POST_INFO_ASPECT_NAME));

    for (Map.Entry<Urn, EntityResponse> entry : postResponses.entrySet()) {
      Urn postUrn = entry.getKey();
      EntityResponse response = entry.getValue();

      if (response == null || !response.getAspects().containsKey(Constants.POST_INFO_ASPECT_NAME)) {
        log.warn("Post {} has no postInfo aspect, skipping", postUrn);
        continue;
      }

      EnvelopedAspect postInfoAspect = response.getAspects().get(Constants.POST_INFO_ASPECT_NAME);
      PostInfo postInfo = new PostInfo(postInfoAspect.getValue().data());

      // Only process posts with HOME_PAGE_ANNOUNCEMENT and LINK types
      if (!PostType.HOME_PAGE_ANNOUNCEMENT.equals(postInfo.getType())
          || !PostContentType.LINK.equals(postInfo.getContent().getType())
          || postInfo.getContent().getLink() == null) {
        continue;
      }

      // Create a page module from this post
      String moduleId = postUrn.getEntityKey().get(0); // Use the same ID from the post URN
      Urn moduleUrn =
          UrnUtils.getUrn("urn:li:" + Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME + ":" + moduleId);

      DataHubPageModuleProperties moduleProperties = createModuleFromPost(postInfo, auditStamp);

      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(moduleUrn);
      proposal.setEntityType(Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME);
      proposal.setAspectName(Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(moduleProperties));
      proposal.setChangeType(ChangeType.UPSERT);

      entityService.ingestProposal(
          systemOperationContext,
          AspectsBatchImpl.builder()
              .mcps(
                  List.of(proposal),
                  new AuditStamp()
                      .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                      .setTime(System.currentTimeMillis()),
                  systemOperationContext.getRetrieverContext())
              .build(systemOperationContext),
          false);

      moduleUrns.add(moduleUrn);
    }

    return moduleUrns;
  }

  private DataHubPageModuleProperties createModuleFromPost(
      @Nonnull PostInfo postInfo, @Nonnull AuditStamp auditStamp) {
    DataHubPageModuleProperties properties = new DataHubPageModuleProperties();

    // Set module name from post title
    properties.setName(postInfo.getContent().getTitle());
    properties.setType(DataHubPageModuleType.LINK);

    // Set visibility
    DataHubPageModuleVisibility visibility = new DataHubPageModuleVisibility();
    visibility.setScope(PageModuleScope.GLOBAL);
    properties.setVisibility(visibility);

    // Set link parameters
    DataHubPageModuleParams params = new DataHubPageModuleParams();
    LinkModuleParams linkParams = new LinkModuleParams();

    linkParams.setLinkUrl(postInfo.getContent().getLink().toString());
    if (postInfo.getContent().getDescription() != null) {
      linkParams.setDescription(postInfo.getContent().getDescription());
    }
    if (postInfo.getContent().getMedia() != null
        && postInfo.getContent().getMedia().getLocation() != null) {
      linkParams.setImageUrl(postInfo.getContent().getMedia().getLocation().toString());
    }

    params.setLinkParams(linkParams);
    properties.setParams(params);

    // Set audit stamps
    properties.setCreated(auditStamp);
    properties.setLastModified(auditStamp);

    return properties;
  }

  private void updateHomePageTemplate(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull Urn templateUrn,
      @Nonnull DataHubPageTemplateProperties templateProperties,
      @Nonnull List<Urn> moduleUrns,
      @Nonnull AuditStamp auditStamp)
      throws Exception {

    // Create rows for the modules (max 3 modules per row)
    List<DataHubPageTemplateRow> newRows = new ArrayList<>();
    for (int i = 0; i < moduleUrns.size(); i += 3) {
      DataHubPageTemplateRow row = new DataHubPageTemplateRow();
      List<Urn> rowModules = moduleUrns.subList(i, Math.min(i + 3, moduleUrns.size()));
      row.setModules(new UrnArray(rowModules));
      newRows.add(row);
    }

    // Insert the new rows at the beginning of the existing rows
    List<DataHubPageTemplateRow> existingRows =
        templateProperties.getRows() != null
            ? new ArrayList<>(templateProperties.getRows())
            : new ArrayList<>();

    newRows.addAll(existingRows);
    templateProperties.setRows(new DataHubPageTemplateRowArray(newRows));
    templateProperties.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(templateUrn);
    proposal.setEntityType(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(templateProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    entityService.ingestProposal(
        systemOperationContext,
        AspectsBatchImpl.builder()
            .mcps(
                List.of(proposal),
                new AuditStamp()
                    .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()),
                systemOperationContext.getRetrieverContext())
            .build(systemOperationContext),
        false);
  }
}
