package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultMetadata;
import com.linkedin.metadata.query.Filter;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.util.LinkedList;
import javax.annotation.Nonnull;


/**
 * A base class for browse rest.li resource
 *
 * @deprecated Use {@link BaseBrowsableEntityResource} instead
 */
public abstract class BaseBrowseResource extends CollectionResourceTaskTemplate<String, BrowseResultEntity> {

  /**
   * Returns a specific {@link BaseBrowseDAO}.
   */
  @Nonnull
  protected abstract BaseBrowseDAO getBrowseDAO();

  /**
   * Gets browse content given entity type and current path
   *
   * @param inputPath current path to be browsed
   * @param filter browse request
   * @param pagingContext the pagination context
   * @return entities and groups in the current path and their counts
   */
  @Finder("path")
  public Task<CollectionResult<BrowseResultEntity, BrowseResultMetadata>> browse(
      @QueryParam("inputPath") @Nonnull String inputPath, @QueryParam("filter") @Nonnull Filter filter,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return RestliUtils.toTask(() -> {
      final BrowseResult browseResult =
          getBrowseDAO().browse(inputPath, filter, pagingContext.getStart(), pagingContext.getCount());

      return new CollectionResult<>(new LinkedList<>(browseResult.getEntities()), browseResult.getNumEntities(),
          browseResult.getMetadata());
    });
  }

  /**
   * Gets browse path(s) given an entity urn
   *
   * @param urn urn for entity
   * @return list of paths given an entity urn
   */
  @Action(name = "getBrowsePaths")
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    return RestliUtils.toTask(() -> new StringArray(getBrowseDAO().getBrowsePaths(urn)));
  }
}
