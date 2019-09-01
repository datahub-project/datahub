package com.linkedin.datahub.models.view;

import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class DatasetColumn {

  private Long id;

  private int sortID;

  private int parentSortID;

  private String fieldName;

  private String parentPath;

  private String fullFieldPath;

  private String dataType;

  private String comment;

  private Long commentCount;

  private String partitionedStr;

  private boolean partitioned;

  private String nullableStr;

  private boolean nullable;

  private String indexedStr;

  private boolean indexed;

  private String distributedStr;

  private boolean distributed;

  private String treeGridClass;
}
