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
package wherehows.processors;

import lombok.RequiredArgsConstructor;
import wherehows.dao.DaoFactory;
import wherehows.service.GobblinTrackingAuditService;
import wherehows.service.JobExecutionLineageService;
import wherehows.service.MetadataChangeService;
import wherehows.service.MetadataInventoryService;


@RequiredArgsConstructor
public class ProcessorFactory {
  private final DaoFactory daoFactory;

  public GobblinTrackingAuditProcessor getGobblinTrackingAuditProcessor() {
    GobblinTrackingAuditService service =
        new GobblinTrackingAuditService(daoFactory.getDatasetClassificationDao(), daoFactory.getDictDatasetDao());
    return new GobblinTrackingAuditProcessor(service);
  }

  public JobExecutionLineageProcessor getJobExecutionLineageProcessor() {
    return new JobExecutionLineageProcessor(new JobExecutionLineageService());
  }

  public MetadataChangeProcessor getMetadataChangeProcessor() {
    return new MetadataChangeProcessor(new MetadataChangeService(daoFactory.getDictDatasetDao(), daoFactory.getDictFieldDetailDao(), daoFactory.getDatasetSchemaInfoDao()));
  }

  public MetadataInventoryProcessor getMetadataInventoryProcessor() {
    return new MetadataInventoryProcessor(new MetadataInventoryService());
  }
}
