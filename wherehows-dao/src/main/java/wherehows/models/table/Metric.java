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
import javax.persistence.Table;
import javax.persistence.Transient;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@NoArgsConstructor
@Table(name = "dict_business_metric")
public class Metric {

  @Id
  @Column(name = "metric_id")
  private Integer id;

  @Column(name = "metric_name")
  private String name;

  @Column(name = "metric_description")
  private String description;

  @Column(name = "dashboard_name")
  private String dashboardName;

  @Column(name = "metric_group")
  private String group;

  @Column(name = "metric_category")
  private String category;

  @Column(name = "metric_sub_category")
  private String subCategory;

  @Column(name = "metric_level")
  private String level;

  @Column(name = "metric_source_type")
  private String sourceType;

  @Column(name = "metric_source")
  private String source;

  @Column(name = "metric_source_dataset_id")
  private Long sourceDatasetId;

  @Column(name = "metric_ref_id_type")
  private String refIDType;

  @Column(name = "metric_ref_id")
  private String refID;

  @Column(name = "metric_type")
  private String type;

  @Column(name = "metric_grain")
  private String grain;

  @Column(name = "metric_display_factor")
  private String displayFactor;

  @Column(name = "metric_display_factor_sym")
  private String displayFactorSym;

  @Column(name = "metric_good_direction")
  private String goodDirection;

  @Column(name = "metric_formula")
  private String formula;

  @Column(name = "dimensions")
  private String dimensions;

  @Column(name = "owners")
  private String owners;

  @Column(name = "tags")
  private String tags;

  @Column(name = "urn")
  private String urn;

  @Column(name = "metric_url")
  private String metricUrl;

  @Column(name = "wiki_url")
  private String wikiUrl;

  @Column(name = "scm_url")
  private String scmUrl;

  @Transient
  private String schema;

  @Transient
  private Long watchId;

  @Transient
  private String sourceDatasetLink;
}
