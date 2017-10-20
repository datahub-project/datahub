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
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class DatasetView {

  private String platform;

  private String nativeName;

  private String fabric;

  private String uri;

  private String description;

  private String nativeType;

  private String properties;

  private List<String> tags;

  private Boolean removed;

  private Boolean deprecated;

  private String deprecationNote;

  private Long createdTime;

  private Long modifiedTime;
}
