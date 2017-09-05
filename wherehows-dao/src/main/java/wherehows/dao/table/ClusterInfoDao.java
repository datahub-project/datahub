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

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import lombok.SneakyThrows;
import wherehows.models.table.ClusterInfo;


public class ClusterInfoDao extends BaseDao {

  public ClusterInfoDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public List<ClusterInfo> findAll() {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<ClusterInfo> criteria = cb.createQuery(ClusterInfo.class);
    Root<ClusterInfo> entityRoot = criteria.from(ClusterInfo.class);
    criteria.select(entityRoot);
    try {
      return entityManager.createQuery(criteria)
          .getResultList();
    } finally {
      entityManager.close();
    }

  }
}
