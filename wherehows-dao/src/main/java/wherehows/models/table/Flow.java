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

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Transient;
import lombok.Data;


@Data
@Entity
@IdClass(value = Flow.FlowKeys.class)
@Table(name = "flow")
public class Flow {

  @Id
  @Column(name = "flow_id")
  private Long flow_id;

  @Id
  @Column(name = "app_id")
  private Integer appId;

  @Column(name = "flow_name")
  private String name;

  @Column(name = "flow_group")
  private String group;

  @Column(name = "flow_path")
  private String path;

  @Column(name = "flow_level")
  private Integer level;

  @Transient
  private int jobCount;

  @Transient
  private String created;

  @Transient
  private String modified;

  @Transient
  private String appCode;

  static class FlowKeys implements Serializable {
    private String flow_id;
    private String appId;
  }
}
