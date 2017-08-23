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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.models.DatasetOwner;


/**
 * Intended to be used for Front-end readonly functions
 * Use native SQL and then map to entity
 */
public class MetadataReadOnlyDao {

  private static final Logger log = LoggerFactory.getLogger(MetadataReadOnlyDao.class);

  private final EntityManagerFactory _emFactory;

  public MetadataReadOnlyDao(EntityManagerFactory factory) {
    this._emFactory = factory;
  }

  private static final String GET_DATASET_URN_BY_ID = "SELECT urn FROM dict_dataset WHERE id = :id";

  private final static String GET_DATASET_OWNERS_BY_ID =
      "SELECT o.owner_id, u.display_name, o.sort_id, o.owner_type, o.namespace, o.owner_id_type, o.owner_source, "
          + "o.owner_sub_type, o.confirmed_by, u.email, u.is_active, is_group, o.modified_time "
          + "FROM dataset_owner o "
          + "LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) "
          + "WHERE o.dataset_id = :datasetId and (o.is_deleted is null OR o.is_deleted != 'Y') ORDER BY o.sort_id";

  /**
   * get dataset URN by dataset ID
   * @param datasetId int
   * @return URN String, if not found, return null
   */
  public String getDatasetUrnById(int datasetId) {
    Map<String, Object> params = new HashMap<>();
    params.put("id", datasetId);

    List<Object> result = getObjectListBy(GET_DATASET_URN_BY_ID, params);
    if (result == null || result.size() == 0) {
      log.error("Can not find URN for dataset id: " + datasetId + " : ");
      return null;
    }
    return (String) result.get(0);
  }

  /**
   * Get dataset owner list by dataset ID
   * @param datasetId int
   * @return List of DatasetOwner
   */
  public List<DatasetOwner> getDatasetOwnersByID(int datasetId) {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetId", datasetId);

    List<DatasetOwner> owners = getEntityListBy(GET_DATASET_OWNERS_BY_ID, DatasetOwner.class, params);
    for (DatasetOwner owner : owners) {
      owner.setModifiedTime(owner.getModifiedTime() * 1000);
    }
    return owners;
  }

  /**
   * get dataset URN by dataset ID and do simple validate
   * @param datasetId int
   * @return valid Wherehows URN
   * @throws IllegalArgumentException when dataset URN not found or invalid
   */
  public String validateUrn(int datasetId) throws IllegalArgumentException {
    String urn = getDatasetUrnById(datasetId);
    if (urn == null || urn.length() < 6 || urn.split(":///").length != 2) {
      throw new IllegalArgumentException("Dataset id not found: " + datasetId);
    }
    return urn;
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
  protected <T> List<T> getEntityListBy(String sqlQuery, Class classType, Map<String, Object> params) {
    EntityManager entityManager = _emFactory.createEntityManager();
    Query query = entityManager.createNativeQuery(sqlQuery, classType);
    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    try {
      return (List<T>) query.getResultList();
    } finally {
      entityManager.close();
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
  protected <T> T getEntityBy(String sqlQuery, Class classType, Map<String, Object> params) {
    EntityManager entityManager = _emFactory.createEntityManager();
    Query query = entityManager.createNativeQuery(sqlQuery, classType);
    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    try {
      return (T) query.getSingleResult();
    } finally {
      entityManager.close();
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
  protected List<Object[]> getObjectArrayListBy(String sqlQuery, Map<String, Object> params) {
    EntityManager entityManager = _emFactory.createEntityManager();
    Query query = entityManager.createNativeQuery(sqlQuery);
    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    try {
      return query.getResultList();
    } finally {
      entityManager.close();
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
  protected List<Object> getObjectListBy(String sqlQuery, Map<String, Object> params) {
    EntityManager entityManager = _emFactory.createEntityManager();
    Query query = entityManager.createNativeQuery(sqlQuery);
    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    try {
      return query.getResultList();
    } finally {
      entityManager.close();
    }
  }
}
