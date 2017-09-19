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
@Table(name = "dict_dataset_schema_history")
public class SchemaDataset {

  @Id
  @Column(name = "id")
  private int id;

  @Column(name = "dataset_id")
  private int dataset_id;

  @Transient
  private String name;

  @Column(name = "urn")
  private String urn;

  @Column(name = "modified_date")
  private String lastModified;

  @Transient
  private String datasetLink;
}