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
package wherehows.models.table;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "dataset_compliance")
@NoArgsConstructor
public class DsCompliance {

  @Id
  @Column(name = "dataset_id", nullable = false)
  private int datasetId;

  @Column(name = "dataset_urn", nullable = false)
  private String datasetUrn;

  @Column(name = "compliance_purge_type")
  private String compliancePurgeType;

  @Column(name = "compliance_purge_note")
  private String compliancePurgeNote;

  @Column(name = "compliance_entities")
  private String complianceEntities;

  @Column(name = "confidentiality")
  private String confidentiality;

  @Column(name = "dataset_classification")
  private String datasetClassification;

  @Column(name = "modified_by")
  private String modifiedBy;

  @Column(name = "modified_time")
  private Integer modifiedTime;

  @PreUpdate
  @PrePersist
  void prePersist() {
    this.modifiedTime = (int) (System.currentTimeMillis() / 1000);
  }
}
