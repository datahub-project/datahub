/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.models.view;

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class DatasetCompliance {

  private Integer datasetId;

  private String datasetUrn;

  private String complianceType;

  private String compliancePurgeNote;

  private List<DatasetFieldEntity> complianceEntities;

  private String confidentiality;

  private Map<String, Boolean> datasetClassification;

  private Boolean containingPersonalData;

  private String modifiedBy;

  private Long modifiedTime;

  private Boolean fromUpstream;
}
