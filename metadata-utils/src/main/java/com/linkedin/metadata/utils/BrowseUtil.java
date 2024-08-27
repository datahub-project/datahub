package com.linkedin.metadata.utils;

import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultEntityArray;
import com.linkedin.metadata.query.BrowseResultGroup;
import com.linkedin.metadata.query.BrowseResultGroupArray;
import com.linkedin.metadata.query.BrowseResultMetadata;
import java.util.stream.Collectors;

public class BrowseUtil {
  private BrowseUtil() {}

  public static com.linkedin.metadata.query.BrowseResult convertToLegacyResult(
      BrowseResult browseResult) {
    com.linkedin.metadata.query.BrowseResult legacyResult =
        new com.linkedin.metadata.query.BrowseResult();

    legacyResult.setFrom(browseResult.getFrom());
    legacyResult.setPageSize(browseResult.getPageSize());
    legacyResult.setNumEntities(browseResult.getNumEntities());
    legacyResult.setEntities(
        new BrowseResultEntityArray(
            browseResult.getEntities().stream()
                .map(entity -> new BrowseResultEntity(entity.data()))
                .collect(Collectors.toList())));

    BrowseResultMetadata legacyMetadata = new BrowseResultMetadata();
    legacyMetadata.setGroups(
        new BrowseResultGroupArray(
            browseResult.getGroups().stream()
                .map(group -> new BrowseResultGroup(group.data()))
                .collect(Collectors.toList())));
    legacyMetadata.setPath(browseResult.getMetadata().getPath());
    legacyMetadata.setTotalNumEntities(browseResult.getMetadata().getTotalNumEntities());

    return legacyResult;
  }
}
