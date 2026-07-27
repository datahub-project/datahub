package com.linkedin.datahub.upgrade.system.homepagelinks;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
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
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MigrateHomePageLinksStep implements UpgradeStep {

  // STEP_ID and VERSION must match the values used when this step ran as a GMS boot step.
  // The idempotency check derives a URN from STEP_ID — changing either value causes all existing
  // deployments to re-run the migration because the prior completion record won't be found.
  private static final String STEP_ID = "migrate-homepage-links";
  private static final String VERSION = "1";
  private static final String DEFAULT_HOME_PAGE_TEMPLATE_URN =
      "urn:li:dataHubPageTemplate:home_default_1";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;
  private final Urn _upgradeUrn;

  public MigrateHomePageLinksStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService) {
    _entityService = entityService;
    _entitySearchService = entitySearchService;
    _upgradeUrn =
        EntityKeyUtils.convertEntityKeyToUrn(
            new DataHubUpgradeKey().setId(STEP_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    try {
      EntityResponse response =
          _entityService.getEntityV2(
              context.opContext(),
              Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
              _upgradeUrn,
              Collections.singleton(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME)) {
        log.info("Step {} already completed. Skipping.", STEP_ID);
        return true;
      }
    } catch (Exception e) {
      log.error("Error checking upgrade history for {}. Proceeding with upgrade.", STEP_ID, e);
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        ingestUpgradeRequest(context.opContext());
        upgrade(context.opContext());
        ingestUpgradeResult(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to migrate home page links", e);
        _entityService.deleteUrn(context.opContext(), _upgradeUrn);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void ingestUpgradeRequest(@Nonnull final OperationContext opContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(_upgradeUrn);
    proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new DataHubUpgradeRequest()
                .setTimestampMs(System.currentTimeMillis())
                .setVersion(VERSION)));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    _entityService.ingestProposal(opContext, proposal, auditStamp, false);
  }

  private void ingestUpgradeResult(@Nonnull final OperationContext opContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(_upgradeUrn);
    proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis())));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    _entityService.ingestProposal(opContext, proposal, auditStamp, false);
  }

  private void upgrade(@Nonnull OperationContext systemOperationContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    log.info("Starting migration of home page links to page modules...");

    List<Urn> postUrns = fetchPostUrns(systemOperationContext);
    log.info("Found {} home page announcement posts to migrate", postUrns.size());

    if (postUrns.isEmpty()) {
      log.info("No home page announcement posts found. Skipping migration.");
      return;
    }

    Urn templateUrn = UrnUtils.getUrn(DEFAULT_HOME_PAGE_TEMPLATE_URN);
    DataHubPageTemplateProperties templateProperties =
        fetchPageTemplateProperties(systemOperationContext, templateUrn);

    if (templateProperties == null) {
      log.warn("Default home page template not found. Skipping migration.");
      return;
    }

    List<Urn> moduleUrns = convertPostsToPageModules(systemOperationContext, postUrns, auditStamp);
    log.info("Created {} page modules from home page posts", moduleUrns.size());

    if (moduleUrns.isEmpty()) {
      return;
    }

    updateHomePageTemplate(
        systemOperationContext, templateUrn, templateProperties, moduleUrns, auditStamp);
    log.info("Updated home page template with migrated modules");

    log.info("Home page links migration completed successfully");
  }

  private List<Urn> fetchPostUrns(@Nonnull OperationContext systemOperationContext) {
    List<Urn> allUrns = new ArrayList<>();
    int start = 0;
    SearchResult searchResult;
    do {
      searchResult =
          _entitySearchService.search(
              systemOperationContext.withSearchFlags(
                  flags ->
                      flags.setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true)),
              List.of(Constants.POST_ENTITY_NAME),
              "*",
              null,
              null,
              start,
              BATCH_SIZE);
      List<Urn> batch =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toList());
      allUrns.addAll(batch);
      start += batch.size();
    } while (start < searchResult.getNumEntities());
    return allUrns;
  }

  private DataHubPageTemplateProperties fetchPageTemplateProperties(
      @Nonnull OperationContext systemOperationContext, @Nonnull Urn templateUrn) {
    try {
      EntityResponse response =
          _entityService.getEntityV2(
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

    Map<Urn, EntityResponse> postResponses =
        _entityService.getEntitiesV2(
            systemOperationContext,
            Constants.POST_ENTITY_NAME,
            new HashSet<>(postUrns),
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

      if (!PostType.HOME_PAGE_ANNOUNCEMENT.equals(postInfo.getType())
          || !PostContentType.LINK.equals(postInfo.getContent().getType())
          || postInfo.getContent().getLink() == null) {
        continue;
      }

      String moduleId = postUrn.getEntityKey().get(0);
      Urn moduleUrn =
          UrnUtils.getUrn("urn:li:" + Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME + ":" + moduleId);

      DataHubPageModuleProperties moduleProperties = createModuleFromPost(postInfo, auditStamp);

      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(moduleUrn);
      proposal.setEntityType(Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME);
      proposal.setAspectName(Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(moduleProperties));
      proposal.setChangeType(ChangeType.UPSERT);

      _entityService.ingestProposal(
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

    properties.setName(postInfo.getContent().getTitle());
    properties.setType(DataHubPageModuleType.LINK);

    DataHubPageModuleVisibility visibility = new DataHubPageModuleVisibility();
    visibility.setScope(PageModuleScope.GLOBAL);
    properties.setVisibility(visibility);

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

    List<DataHubPageTemplateRow> newRows = new ArrayList<>();
    for (int i = 0; i < moduleUrns.size(); i += 3) {
      DataHubPageTemplateRow row = new DataHubPageTemplateRow();
      List<Urn> rowModules = moduleUrns.subList(i, Math.min(i + 3, moduleUrns.size()));
      row.setModules(new UrnArray(rowModules));
      newRows.add(row);
    }

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

    _entityService.ingestProposal(
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
