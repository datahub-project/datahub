package com.linkedin.metadata.resources.entity;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.IngestionRunSummaryArray;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiCollection(name = "runs", namespace = "com.linkedin.entity")
public class IngestionRunResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {

  private static final Integer DEFAULT_MIN_RUN_SIZE = 0;
  private static final Integer DEFAULT_MAX_RUN_SIZE = Integer.MAX_VALUE;
  private static final Integer DEFAULT_OFFSET = 0;
  private static final Integer DEFAULT_PAGE_SIZE = 100;


  @Inject
  @Named("entityService")
  private EntityService _entityService;

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @Action(name = "rollback")
  @Nonnull
  public Task<AspectRowSummaryArray> rollback(
      @ActionParam("runId") @Nonnull String runId,
      @ActionParam("dryRun") @Optional @Nullable Boolean dryRun
  ) throws URISyntaxException {
    log.info("ROLLBACK RUN runId: {} dry run: {}", runId, dryRun);
    return RestliUtils.toTask(() ->
      new AspectRowSummaryArray(_entityService.rollbackRun(runId, dryRun != null ? dryRun : false))
    );
  }

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @Action(name = "ghost")
  @Nonnull
  public Task<StringArray> ghost(
      @ActionParam("runId") @Nonnull String runId,
      @ActionParam("dryRun") @Optional @Nullable Boolean dryRun
  ) {
    log.info("GHOST RUN runId: {} dry run: {}", runId, dryRun);
    return RestliUtils.toTask(() ->
        new StringArray(_entityService.ghostRun(runId, dryRun != null ? dryRun : false))
    );
  }

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @Action(name = "list")
  @Nonnull
  public Task<IngestionRunSummaryArray> list(
      @ActionParam("minRunSize") @Optional @Nullable Integer minRunSize,
      @ActionParam("maxRunSize") @Optional @Nullable Integer maxRunSize,
      @ActionParam("pageOffset") @Optional @Nullable Integer pageOffset,
      @ActionParam("pageSize") @Optional @Nullable Integer pageSize
  ) {
    log.info("LIST RUNS min size: {} max size: {} offset: {} size: {}");

    return RestliUtils.toTask(() ->
      new IngestionRunSummaryArray(_entityService.listRuns(
          minRunSize != null ? minRunSize : DEFAULT_MIN_RUN_SIZE,
          maxRunSize != null ? maxRunSize : DEFAULT_MAX_RUN_SIZE,
          pageOffset != null ? pageOffset : DEFAULT_OFFSET,
          pageSize != null ? pageSize : DEFAULT_PAGE_SIZE
      ))
    );
  }
}
