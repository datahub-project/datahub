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

import org.testng.annotations.Test;
import wherehows.dao.view.DatasetViewDao;
import wherehows.models.table.DictDataset;
import wherehows.models.view.DatasetView;

import static org.testng.Assert.*;


public class DatasetViewDaoTest {

  @Test
  public void testFillDatasetViewFromDictDataset() {
    DatasetViewDao datasetViewDao = new DatasetViewDao(null);

    String urn = "hdfs:///foo/bar";
    int createTime = 1;
    int modifiedTime = 2;
    String type = "Avro";
    String properties = "{properties}";
    boolean active = false;
    boolean deprecated = true;

    DictDataset ds = new DictDataset();
    ds.setUrn(urn);
    ds.setStorageType(type);
    ds.setCreatedTime(createTime);
    ds.setModifiedTime(modifiedTime);
    ds.setProperties(properties);
    ds.setIsActive(active);
    ds.setIsDeprecated(deprecated);

    DatasetView view = datasetViewDao.fillDatasetViewFromDictDataset(ds);

    assertEquals(view.getPlatform(), "hdfs");
    assertEquals(view.getNativeName(), "/foo/bar");
    assertEquals(view.getNativeType(), type);
    assertEquals(view.getUri(), urn);
    assertTrue(view.getRemoved());
    assertTrue(view.getDeprecated());
    assertEquals(view.getProperties(), properties);
    assertEquals(view.getCreatedTime(), new Long(createTime * 1000));
    assertEquals(view.getModifiedTime(), new Long(modifiedTime * 1000));
  }
}
