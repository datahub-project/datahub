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

import javax.persistence.EntityManagerFactory;
import wherehows.dao.table.DatasetComplianceDao;
import wherehows.dao.table.DatasetOwnerDao;
import wherehows.dao.table.DatasetsDao;
import wherehows.dao.table.DictDatasetDao;
import wherehows.dao.table.FieldDetailDao;
import wherehows.dao.table.AclDao;
import wherehows.dao.table.LineageDao;
import wherehows.dao.view.DataTypesViewDao;
import wherehows.dao.view.DatasetViewDao;
import wherehows.dao.view.OwnerViewDao;


public class DaoFactory {

  protected final EntityManagerFactory entityManagerFactory;

  private static DatasetsDao datasetsDao;

  private static DatasetViewDao datasetViewDao;

  private static DictDatasetDao dictDatasetDao;

  private static FieldDetailDao fieldDetailDao;

  private static DatasetOwnerDao datasetOwnerDao;

  private static OwnerViewDao ownerViewDao;

  private static DatasetComplianceDao datasetComplianceDao;

  private static DataTypesViewDao dataTypesViewDao;

  private static LineageDao lineageDao;

  private static AclDao aclDao;

  public DaoFactory(EntityManagerFactory entityManagerFactory) {
    this.entityManagerFactory = entityManagerFactory;
  }

  public DatasetsDao getDatasetsDao() {
    if (datasetsDao == null) {
      datasetsDao = new DatasetsDao();
    }
    return datasetsDao;
  }

  public DatasetViewDao getDatasetViewDao() {
    if (datasetViewDao == null) {
      datasetViewDao = new DatasetViewDao(entityManagerFactory);
    }
    return datasetViewDao;
  }

  public DictDatasetDao getDictDatasetDao() {
    if (dictDatasetDao == null) {
      dictDatasetDao = new DictDatasetDao(entityManagerFactory);
    }
    return dictDatasetDao;
  }

  public FieldDetailDao getDictFieldDetailDao() {
    if (fieldDetailDao == null) {
      fieldDetailDao = new FieldDetailDao(entityManagerFactory);
    }
    return fieldDetailDao;
  }

  public DatasetOwnerDao getDatasteOwnerDao() {
    if (datasetOwnerDao == null) {
      datasetOwnerDao = new DatasetOwnerDao(entityManagerFactory);
    }
    return datasetOwnerDao;
  }

  public OwnerViewDao getOwnerViewDao() {
    if (ownerViewDao == null) {
      ownerViewDao = new OwnerViewDao(entityManagerFactory);
    }
    return ownerViewDao;
  }

  public DatasetComplianceDao getDatasetComplianceDao() {
    if (datasetComplianceDao == null) {
      datasetComplianceDao = new DatasetComplianceDao(entityManagerFactory);
    }
    return datasetComplianceDao;
  }

  public DataTypesViewDao getDataTypesViewDao() {
    if (dataTypesViewDao == null) {
      dataTypesViewDao = new DataTypesViewDao(entityManagerFactory);
    }
    return dataTypesViewDao;
  }

  public LineageDao getLineageDao() {
    if (lineageDao == null) {
      lineageDao = new LineageDao();
    }
    return lineageDao;
  }

  public AclDao getAclDao() {
    if (aclDao == null) {
      aclDao = new AclDao();
    }
    return aclDao;
  }
}
