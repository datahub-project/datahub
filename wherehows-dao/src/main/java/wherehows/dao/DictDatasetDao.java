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
import wherehows.models.DictDataset;


public class DictDatasetDao extends AbstractDao {

  private static final String FIND_BY_URN = "SELECT * FROM dict_dataset WHERE urn = :urn";
  private static final String FIND_BY_ID = "SELECT * FROM dict_dataset WHERE id = :id";

  public DictDatasetDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public DictDataset findByUrn(String urn) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return (DictDataset) entityManager.createQuery(FIND_BY_URN)
          .setParameter("urn", urn)
          .getResultList()
          .get(0);
    } finally {
      entityManager.close();
    }
  }

  @SneakyThrows
  public DictDataset findById(int datasetId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return (DictDataset) entityManager.createQuery(FIND_BY_ID)
          .setParameter("id", datasetId)
          .getResultList()
          .get(0);
    } finally {
      entityManager.close();
    }
  }
}
