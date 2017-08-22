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
import java.util.Objects;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import lombok.SneakyThrows;
import wherehows.models.DictFieldDetail;


public class FieldDetailDao extends BaseDao {

  private static final String DELETE_BY_DATASET_ID = "DELETE FROM dict_field_detail WHERE dataset_id = :datasetId";

  public FieldDetailDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public List<DictFieldDetail> findById(int datasetId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<DictFieldDetail> criteria = cb.createQuery(DictFieldDetail.class);
    Root<DictFieldDetail> entityRoot = criteria.from(DictFieldDetail.class);
    criteria.select(entityRoot);
    criteria.where(cb.equal(entityRoot.get("dataset_Id"), datasetId));
    try {
      return entityManager.createQuery(criteria).getResultList();
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
