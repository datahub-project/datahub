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
package wherehows.dao;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import lombok.SneakyThrows;
import wherehows.models.DatasetSchemaInfo;


public class DatasetSchemaInfoDao extends AbstractDao {

  private static final String FIND_BY_DATASET_ID = "SELECT * FROM dataset_schema_info WHERE dataset_id = :datasetId";

  public DatasetSchemaInfoDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public DatasetSchemaInfo findById(int datasetId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return entityManager.createQuery(FIND_BY_DATASET_ID, DatasetSchemaInfo.class)
          .setParameter("dataset_Id", datasetId)
          .getResultList()
          .get(0);
    } finally {
      entityManager.close();
    }
  }
}
