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
package wherehows.dao.view;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import lombok.SneakyThrows;


/**
 * Base DAO class for view (read only) model, containing generic functions
 * Use native SQL and then map to entity
 */
public class BaseViewDao {

  private final EntityManagerFactory _emFactory;

  public BaseViewDao(@Nonnull EntityManagerFactory factory) {
    this._emFactory = factory;
  }

  /**
   * generic function to fetch a list of entities using native SQL with named parameters.
   * @param sqlQuery SQL query string
   * @param classType T.class the return class type
   * @param params named parameters map
   * @param <T> Generic return Data type
   * @return List of records T
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  protected <T> List<T> getEntityListBy(@Nonnull String sqlQuery, @Nonnull Class<T> classType,
      @Nonnull Map<String, Object> params) {
    EntityManager entityManager = null;
    try {
      entityManager = _emFactory.createEntityManager();

      Query query = entityManager.createNativeQuery(sqlQuery, classType);
      for (Map.Entry<String, Object> param : params.entrySet()) {
        query.setParameter(param.getKey(), param.getValue());
      }

      return (List<T>) query.getResultList();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * generic function to fetch a single entity using native SQL with named parameters.
   * @param sqlQuery SQL query string
   * @param classType T.class the return class type
   * @param params named parameters map
   * @param <T> Generic return Data type
   * @return a single record T
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  protected <T> T getEntityBy(@Nonnull String sqlQuery, @Nonnull Class<T> classType,
      @Nonnull Map<String, Object> params) {
    EntityManager entityManager = null;
    try {
      entityManager = _emFactory.createEntityManager();

      Query query = entityManager.createNativeQuery(sqlQuery, classType);
      for (Map.Entry<String, Object> param : params.entrySet()) {
        query.setParameter(param.getKey(), param.getValue());
      }

      return (T) query.getSingleResult();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * generic function to fetch records using native SQL with named parameters and return multiple column
   * @param sqlQuery SQL query string
   * @param params named parameters map
   * @return List of Object[]
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  protected List<Object[]> getObjectArrayListBy(@Nonnull String sqlQuery, @Nonnull Map<String, Object> params) {
    EntityManager entityManager = null;
    try {
      entityManager = _emFactory.createEntityManager();

      Query query = entityManager.createNativeQuery(sqlQuery);
      for (Map.Entry<String, Object> param : params.entrySet()) {
        query.setParameter(param.getKey(), param.getValue());
      }

      return (List<Object[]>) query.getResultList();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }

  /**
   * generic function to fetch records using native SQL with named parameters and return single column
   * @param sqlQuery SQL query string
   * @param params named parameters map
   * @return List of Object
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  protected List<Object> getObjectListBy(@Nonnull String sqlQuery, @Nonnull Map<String, Object> params) {
    EntityManager entityManager = null;
    try {
      entityManager = _emFactory.createEntityManager();

      Query query = entityManager.createNativeQuery(sqlQuery);
      for (Map.Entry<String, Object> param : params.entrySet()) {
        query.setParameter(param.getKey(), param.getValue());
      }

      return (List<Object>) query.getResultList();
    } finally {
      if (entityManager != null) {
        entityManager.close();
      }
    }
  }
}
