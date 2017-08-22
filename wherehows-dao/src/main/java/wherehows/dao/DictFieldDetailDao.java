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

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import lombok.SneakyThrows;
import wherehows.models.DictFieldDetail;


public class DictFieldDetailDao extends AbstractDao {

  private static final String FIND_BY_DATASET_ID = "SELECT * FROM dict_field_detail WHERE dataset_id = :datasetId";
  private static final String DELETE_BY_DATASET_ID = "DELETE FROM dict_field_detail WHERE dataset_id = :datasetId";

  public DictFieldDetailDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public List<DictFieldDetail> findById(int datasetId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return entityManager.createQuery(FIND_BY_DATASET_ID, DictFieldDetail.class)
          .setParameter("dataset_Id", datasetId)
          .getResultList();
    } finally {
      entityManager.close();
    }
  }

  @SneakyThrows
  public void deleteByDatasetId(int datasetId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      entityManager.createQuery(DELETE_BY_DATASET_ID).executeUpdate();
    } finally {
      entityManager.close();
    }
  }
}
