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

import com.linkedin.events.metadata.OwnerCategory;
import com.linkedin.events.metadata.OwnerInfo;
import com.linkedin.events.metadata.OwnerType;
import com.linkedin.events.metadata.OwnershipProvider;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;
import wherehows.dao.table.DatasetOwnerDao;
import wherehows.models.table.DsOwner;

import static org.testng.Assert.*;


public class DatasetOwnerDaoTest {

  @Test
  public void testFillDsOwnerByOwnerInfo() {
    DatasetOwnerDao ownerDao = new DatasetOwnerDao(null);

    DsOwner dsOwner = new DsOwner();

    OwnerInfo ownerInfo = new OwnerInfo();
    ownerInfo.owner = "tester";
    ownerInfo.ownerType = OwnerType.USER;
    ownerInfo.ownerCategory = OwnerCategory.DATA_OWNER;
    ownerInfo.ownershipProvider = OwnershipProvider.SCM;

    int createdTime = (int) (System.currentTimeMillis() / 1000);

    ownerDao.fillDsOwnerByOwnerInfo(ownerInfo, dsOwner, createdTime);

    assertEquals(dsOwner.getOwnerId(), "tester");
    assertEquals(dsOwner.getAppId(), 300);
    assertEquals(dsOwner.getOwnerIdType(), "USER");
    assertEquals(dsOwner.getNamespace(), "urn:li:corpuser");
    assertEquals(dsOwner.getIsGroup(), "N");
    assertEquals(dsOwner.getIsActive(), "Y");
    assertEquals(dsOwner.getOwnerType(), "Owner");
    assertEquals(dsOwner.getOwnerSource(), "SCM");
    assertEquals(dsOwner.getSourceTime().intValue(), createdTime);

    ownerInfo.owner = "testGroup";
    ownerInfo.ownerType = OwnerType.GROUP;
    ownerInfo.ownerCategory = OwnerCategory.PRODUCER;
    ownerInfo.ownershipProvider = OwnershipProvider.NUAGE;

    int modifiedTime = (int) (System.currentTimeMillis() / 1000);

    ownerDao.fillDsOwnerByOwnerInfo(ownerInfo, dsOwner, modifiedTime);

    assertEquals(dsOwner.getOwnerId(), "testGroup");
    assertEquals(dsOwner.getAppId(), 301);
    assertEquals(dsOwner.getOwnerIdType(), "GROUP");
    assertEquals(dsOwner.getNamespace(), "urn:li:corpGroup");
    assertEquals(dsOwner.getIsGroup(), "Y");
    assertEquals(dsOwner.getIsActive(), "Y");
    assertEquals(dsOwner.getOwnerType(), "Producer");
    assertEquals(dsOwner.getOwnerSource(), "NUAGE");
    assertEquals(dsOwner.getSourceTime().intValue(), modifiedTime);
    assertEquals(dsOwner.getCreatedTime(), null);
    assertEquals(dsOwner.getModifiedTime(), null);
  }

  @Test
  public void testDiffOwnerList() {
    DatasetOwnerDao ownerDao = new DatasetOwnerDao(null);

    int datasetId = 101;
    String datasetUrn = "hive:///abc/test";
    int createdTime = (int) (System.currentTimeMillis() / 1000);

    DsOwner dsOwner1 = new DsOwner();
    dsOwner1.setDatasetId(datasetId);
    dsOwner1.setDatasetUrn(datasetUrn + "2");
    dsOwner1.setOwnerId("tester1");
    dsOwner1.setOwnerIdType("USER");
    dsOwner1.setOwnerType("DATA_OWNER");
    dsOwner1.setOwnerSource("SCM");
    dsOwner1.setCreatedTime(createdTime);
    dsOwner1.setSourceTime(createdTime);
    dsOwner1.setModifiedTime(createdTime);

    DsOwner dsOwner2 = new DsOwner();
    dsOwner2.setDatasetId(datasetId);
    dsOwner2.setDatasetUrn(datasetUrn);
    dsOwner2.setOwnerId("tester2");
    dsOwner2.setOwnerIdType("USER");
    dsOwner2.setOwnerType("DELEGATE");
    dsOwner2.setOwnerSource("DB");

    List<DsOwner> dsOwners = Arrays.asList(dsOwner1, dsOwner2);

    int modifiedTime = (int) (System.currentTimeMillis() / 1000);

    OwnerInfo ownerInfo1 = new OwnerInfo();
    ownerInfo1.owner = "tester1";
    ownerInfo1.ownerType = OwnerType.USER;
    ownerInfo1.ownerCategory = OwnerCategory.PRODUCER;
    ownerInfo1.ownershipProvider = OwnershipProvider.SOS;

    OwnerInfo ownerInfo2 = new OwnerInfo();
    ownerInfo2.owner = "testGroup";
    ownerInfo2.ownerType = OwnerType.SERVICE;
    ownerInfo2.ownerCategory = OwnerCategory.DELEGATE;
    ownerInfo2.ownershipProvider = OwnershipProvider.JIRA;

    List<OwnerInfo> ownerInfoList = Arrays.asList(ownerInfo2, ownerInfo1);

    List<List<DsOwner>> updatedList =
        ownerDao.diffOwnerList(dsOwners, ownerInfoList, datasetId, datasetUrn, modifiedTime);

    List<DsOwner> combined = updatedList.get(0);
    List<DsOwner> removed = updatedList.get(1);

    assertEquals(removed.size(), 1);
    assertEquals(removed.get(0).getOwnerId(), "tester2");
    assertEquals(removed.get(0).getOwnerSource(), "DB");

    assertEquals(combined.size(), 2);
    assertEquals(combined.get(0).getOwnerId(), "testGroup");
    assertEquals(combined.get(0).getDatasetId(), datasetId);
    assertEquals(combined.get(0).getDatasetUrn(), datasetUrn);
    assertEquals(combined.get(0).getOwnerIdType(), "SERVICE");
    assertEquals(combined.get(0).getAppId(), 301);
    assertEquals(combined.get(0).getSourceTime().intValue(), modifiedTime);

    assertEquals(combined.get(1).getOwnerId(), "tester1");
    assertEquals(combined.get(1).getDatasetId(), datasetId);
    assertEquals(combined.get(1).getDatasetUrn(), datasetUrn);
    assertEquals(combined.get(1).getOwnerIdType(), "USER");
    assertEquals(combined.get(1).getAppId(), 300);
    assertEquals(combined.get(1).getSourceTime().intValue(), modifiedTime);
  }
}
