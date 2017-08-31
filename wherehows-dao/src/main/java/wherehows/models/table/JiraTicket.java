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

import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class JiraTicket {

  private String currentAssignee;
  private String currentAssigneeOrgHierarchy;
  private String assigneeDisplayName;
  private String assigneeFullName;
  private String assigneeTitle;
  private String assigneeManagerId;
  private String assigneeEmail;
  private String ticketHdfsName;
  private String ticketDirectoryPath;
  private Long ticketTotalSize;
  private Long ticketNumOfFiles;
  private String ticketKey;
  private String ticketStatus;
  private String ticketComponent;
}
