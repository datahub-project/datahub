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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import lombok.SneakyThrows;
import wherehows.models.table.DictDataset;


public class DictDatasetDao extends BaseDao {

  public DictDatasetDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public DictDataset findByUrn(String urn) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<DictDataset> criteria = cb.createQuery(DictDataset.class);
    Root<DictDataset> entityRoot = criteria.from(DictDataset.class);
    criteria.select(entityRoot);
    criteria.where(cb.equal(entityRoot.get("urn"), urn));
    try {
      return entityManager.createQuery(criteria).getSingleResult();
    } finally {
      entityManager.close();
    }
  }

  @SneakyThrows
  public DictDataset findById(int datasetId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<DictDataset> criteria = cb.createQuery(DictDataset.class);
    Root<DictDataset> entityRoot = criteria.from(DictDataset.class);
    criteria.select(entityRoot);
    criteria.where(cb.equal(entityRoot.get("dataset_id"), datasetId));
    try {
      return entityManager.createQuery(criteria).getSingleResult();
    } finally {
      entityManager.close();
    }
  }
}
