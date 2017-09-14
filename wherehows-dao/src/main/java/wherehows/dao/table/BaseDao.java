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
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import lombok.SneakyThrows;


public class BaseDao {

  final EntityManagerFactory entityManagerFactory;

  public BaseDao(EntityManagerFactory factory) {
    this.entityManagerFactory = factory;
  }

  /**
   * Find an entity by primary key
   * @param entityClass T.class the entity class type
   * @param primaryKey Object
   * @param <T> Entity type to find
   * @return An entity
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  public <T> T find(Class entityClass, Object primaryKey) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return (T) entityManager.find(entityClass, primaryKey);
    } finally {
      entityManager.close();
    }
  }

  /**
   * Find an entity by a single criteria, e.g. id, urn.
   * @param entityClass T.class the entity class type
   * @param criteriaKey String
   * @param criteriaValue Object
   * @param <T> Entity type to find
   * @return An entity
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  public <T> T findBy(Class entityClass, String criteriaKey, Object criteriaValue) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<T> criteria = cb.createQuery(entityClass);
    Root<T> entityRoot = criteria.from(entityClass);
    criteria.select(entityRoot);
    criteria.where(cb.equal(entityRoot.get(criteriaKey), criteriaValue));
    try {
      return entityManager.createQuery(criteria).getSingleResult();
    } finally {
      entityManager.close();
    }
  }

  /**
   * Find a list of entities by a single criteria, e.g. id, urn.
   * @param entityClass T.class the entity class type
   * @param criteriaKey String
   * @param criteriaValue Object
   * @param <T> Entity type to find
   * @return List of entities
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  public <T> List<T> findListBy(Class entityClass, String criteriaKey, Object criteriaValue) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<T> criteria = cb.createQuery(entityClass);
    Root<T> entityRoot = criteria.from(entityClass);
    criteria.select(entityRoot);
    criteria.where(cb.equal(entityRoot.get(criteriaKey), criteriaValue));
    try {
      return entityManager.createQuery(criteria).getResultList();
    } finally {
      entityManager.close();
    }
  }

  /**
   * Update/merge an entity record.
   * @param record an entity object
   */
  @SneakyThrows
  public void update(Object record) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    entityManager.getTransaction().begin();
    try {
      entityManager.merge(record);
      entityManager.getTransaction().commit();
    } finally {
      entityManager.close();
    }
  }

  /**
   * Execute an update or delete query String.
   * @param queryStr String
   * @param params Parameters
   */
  @SneakyThrows
  public void executeUpdate(String queryStr, Map<String, Object> params) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      Query query = entityManager.createQuery(queryStr);
      for (Map.Entry<String, Object> param : params.entrySet()) {
        query.setParameter(param.getKey(), param.getValue());
      }

      query.executeUpdate();
    } finally {
      entityManager.close();
    }
  }
}
