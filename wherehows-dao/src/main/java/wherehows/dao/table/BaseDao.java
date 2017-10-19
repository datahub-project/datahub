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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import lombok.SneakyThrows;


public class BaseDao {

  protected final EntityManagerFactory entityManagerFactory;

  public BaseDao(@Nonnull EntityManagerFactory factory) {
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
  public <T> T find(@Nonnull Class<T> entityClass, @Nonnull Object primaryKey) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();

      return entityManager.find(entityClass, primaryKey);
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
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
  public <T> T findBy(@Nonnull Class<T> entityClass, @Nonnull String criteriaKey, @Nonnull Object criteriaValue) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();

      CriteriaBuilder cb = entityManager.getCriteriaBuilder();
      CriteriaQuery<T> criteria = cb.createQuery(entityClass);
      Root<T> entityRoot = criteria.from(entityClass);
      criteria.select(entityRoot);
      criteria.where(cb.equal(entityRoot.get(criteriaKey), criteriaValue));

      return entityManager.createQuery(criteria).getSingleResult();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
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
  public <T> List<T> findListBy(@Nonnull Class<T> entityClass, @Nonnull String criteriaKey,
      @Nonnull Object criteriaValue) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();

      CriteriaBuilder cb = entityManager.getCriteriaBuilder();
      CriteriaQuery<T> criteria = cb.createQuery(entityClass);
      Root<T> entityRoot = criteria.from(entityClass);
      criteria.select(entityRoot);
      criteria.where(cb.equal(entityRoot.get(criteriaKey), criteriaValue));

      return entityManager.createQuery(criteria).getResultList();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * Find a list of entities by a parameter map
   * @param entityClass T.class the entity class type
   * @param params Map<String, Object>
   * @param <T> Entity type to find
   * @return List of entities
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  public <T> List<T> findListBy(@Nonnull Class<T> entityClass, @Nonnull Map<String, ? extends Object> params) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();

      CriteriaBuilder cb = entityManager.getCriteriaBuilder();
      CriteriaQuery<T> criteria = cb.createQuery(entityClass);
      Root<T> entityRoot = criteria.from(entityClass);

      //Constructing list of parameters
      List<Predicate> predicates = new ArrayList<Predicate>();
      for (Map.Entry<String, ? extends Object> entry : params.entrySet()) {
        predicates.add(cb.equal(entityRoot.get(entry.getKey()), entry.getValue()));
      }

      criteria.select(entityRoot).where(predicates.toArray(new Predicate[]{}));

      return entityManager.createQuery(criteria).getResultList();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * Merge (update or create) an entity record.
   * @param record an entity object
   * @return the persisted / managed record
   */
  @SneakyThrows
  public Object update(@Nonnull Object record) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();

      entityManager.getTransaction().begin();
      Object managedRecord = entityManager.merge(record);
      entityManager.getTransaction().commit();
      return managedRecord;
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * Update/merge a list of entity records.
   * @param records a list of entity objects
   */
  @SneakyThrows
  public void updateList(@Nonnull List<? extends Object> records) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();
      entityManager.getTransaction().begin();

      for (Object record : records) {
        entityManager.merge(record);
        entityManager.flush();
      }
      entityManager.getTransaction().commit();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * Remove an entity record. If it's detached, try to attach it first.
   * @param record an entity object
   */
  @SneakyThrows
  public void remove(@Nonnull Object record) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();

      entityManager.getTransaction().begin();
      entityManager.remove(entityManager.contains(record) ? record : entityManager.merge(record));
      entityManager.getTransaction().commit();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * Remove a list of entity record.
   * @param records a list of entity object
   */
  @SneakyThrows
  public void removeList(@Nonnull List<? extends Object> records) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();
      entityManager.getTransaction().begin();

      for (Object record : records) {
        entityManager.remove(entityManager.contains(record) ? record : entityManager.merge(record));
        entityManager.flush();
      }
      entityManager.getTransaction().commit();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * Execute an update or delete HQL query String, not native query string.
   * @param queryStr String
   * @param params Parameters
   */
  @SneakyThrows
  public void executeUpdate(@Nonnull String queryStr, @Nonnull Map<String, Object> params) {
    EntityManager entityManager = null;
    try {
      entityManager = entityManagerFactory.createEntityManager();
      entityManager.getTransaction().begin();

      Query query = entityManager.createQuery(queryStr);
      for (Map.Entry<String, Object> param : params.entrySet()) {
        query.setParameter(param.getKey(), param.getValue());
      }

      query.executeUpdate();
      entityManager.getTransaction().commit();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }
}
