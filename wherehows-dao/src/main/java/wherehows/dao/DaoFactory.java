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
import wherehows.dao.table.DatasetClassificationDao;
import wherehows.dao.table.DatasetSchemaInfoDao;
import wherehows.dao.table.DatasetsDao;
import wherehows.dao.table.DictDatasetDao;
import wherehows.dao.table.FieldDetailDao;
import wherehows.dao.view.DatasetViewDao;
import wherehows.dao.view.OwnerViewDao;


public class DaoFactory {

  protected final EntityManagerFactory entityManagerFactory;

  private static DatasetsDao datasetsDao;

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
    return new DatasetViewDao(entityManagerFactory);
  }

  public OwnerViewDao getOwnerViewDao() {
    return new OwnerViewDao(entityManagerFactory);
  }

  public DatasetClassificationDao getDatasetClassificationDao() {
    return new DatasetClassificationDao(entityManagerFactory);
  }

  public DictDatasetDao getDictDatasetDao() {
    return new DictDatasetDao(entityManagerFactory);
  }

  public FieldDetailDao getDictFieldDetailDao() {
    return new FieldDetailDao(entityManagerFactory);
  }

  public DatasetSchemaInfoDao getDatasetSchemaInfoDao() {
    return new DatasetSchemaInfoDao(entityManagerFactory);
  }
}
