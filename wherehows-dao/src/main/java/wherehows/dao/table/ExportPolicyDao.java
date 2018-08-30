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
package wherehows.dao.table;

import javax.annotation.Nonnull;
import wherehows.models.view.DatasetExportPolicy;

public class ExportPolicyDao {
  /**
   * Get datset health by dataset urn
   */
  @Nonnull
  public DatasetExportPolicy getDatasetExportPolicy(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void updateDatasetExportPolicy(@Nonnull DatasetExportPolicy record, @Nonnull String user) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");

  }
}