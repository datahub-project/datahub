package com.linkedin.metadata.dao;

import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultMetadata;
import java.util.List;
import lombok.Value;


/*
 * Browse result wrapper with a list of entities and related browse result metadata
 */
@Value
public class BrowseResult {

  // A list of entities under the queried path
  List<BrowseResultEntity> entityList;

  // Contains metadata specific to the browse result of the queried path
  BrowseResultMetadata browseResultMetadata;

  // Offset of the first entity in the result
  int from;

  // Size of each page in the result
  int pageSize;

  // The number of entities directly under queried path
  int numEntities;
}
