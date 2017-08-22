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
import wherehows.models.ClusterInfo;


public class ClusterInfoDao extends AbstractDao {

  private static final String GET_CLUSTER_INFO = "SELECT c FROM ClusterInfo c";

  public ClusterInfoDao(EntityManagerFactory factory) {
    super(factory);
  }

  @SneakyThrows
  public List<ClusterInfo> findAll() {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return entityManager.createQuery(GET_CLUSTER_INFO, ClusterInfo.class).getResultList();
    } finally {
      entityManager.close();
    }
  }
}
