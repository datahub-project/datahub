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

import com.linkedin.events.metadata.DatasetLineage;
import java.util.List;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LineageDao {

  /**
   * Create lineage dataset that requested the lineage via Kafka lineage event.
   * @param datasetLineages List of lineages
   * @return return process result as true/false
   */
  public Boolean createLineages(List<DatasetLineage> datasetLineages) {
    // TODO: write lineage Dao to DB
    throw new UnsupportedOperationException("Lineage not implemented yet.");
  }
}
