package com.linkedin.datahub.models.view;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
public class DatasetOwnership {

  private List<DatasetOwner> owners;

  private Boolean fromUpstream;

  private String datasetUrn;

  private Long lastModified;

  private String actor;
}
