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

import javax.persistence.EntityManagerFactory;
import wherehows.models.table.DictDataset;


public class DictDatasetDao extends BaseDao {

  public DictDatasetDao(EntityManagerFactory factory) {
    super(factory);
  }

  public DictDataset findByUrn(String urn) {
    return findBy(DictDataset.class, "urn", urn);
  }

  public DictDataset findById(int datasetId) {
    return findBy(DictDataset.class, "id", datasetId);
  }
}
