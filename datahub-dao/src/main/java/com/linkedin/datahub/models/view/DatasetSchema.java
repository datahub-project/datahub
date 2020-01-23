package com.linkedin.datahub.models.view;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Setter
@Getter
@NoArgsConstructor
public class DatasetSchema {

  private Boolean schemaless;

  private String rawSchema;

  private String keySchema;

  private List<DatasetColumn> columns;

  private Long lastModified;
}
