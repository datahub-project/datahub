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

import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.OwnerInfo;
import com.linkedin.events.metadata.OwnerType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import wherehows.models.table.DsOwner;


@Slf4j
public class DatasetOwnerDao extends BaseDao {

  public DatasetOwnerDao(EntityManagerFactory factory) {
    super(factory);
  }

  public List<DsOwner> findByUrn(String datasetUrn) {
    return findListBy(DsOwner.class, "datasetUrn", datasetUrn);
  }

  public List<DsOwner> findById(int datasetId) {
    return findListBy(DsOwner.class, "datasetId", datasetId);
  }

  public List<DsOwner> findByIdAndSource(int datasetId, String source) {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetId", datasetId);
    params.put("ownerSource", source);

    return findListBy(DsOwner.class, params);
  }

  /**
   * Insert or update dataset owners given information from MetadataChangeEvent
   * @param datasetId int
   * @param datasetUrn String
   * @param auditStamp ChangeAuditStamp
   * @param owners List<OwnerInfo>
   */
  @SneakyThrows
  public void insertUpdateOwnership(int datasetId, String datasetUrn, ChangeAuditStamp auditStamp,
      List<OwnerInfo> owners) {

    if (owners.size() == 0) {
      throw new IllegalArgumentException("OwnerInfo array is empty!");
    }

    // find dataset owners of same source if exist
    List<DsOwner> dsOwners = findByIdAndSource(datasetId, owners.get(0).ownershipProvider.name());

    List<List<DsOwner>> updatedList =
        diffOwnerList(dsOwners, owners, datasetId, datasetUrn, (int) (auditStamp.time / 1000));

    if (updatedList.get(1).size() > 0) {
      removeList(updatedList.get(1));
    }

    if (updatedList.get(0).size() > 0) {
      updateList(updatedList.get(0));
    }
  }

  /**
   * Fill in DsOwner information from OwnerInfo
   * @param owner OwnerInfo
   * @param dsOwner DsOwner
   * @param sourceTime int
   */
  public void fillDsOwnerByOwnerInfo(OwnerInfo owner, DsOwner dsOwner, int sourceTime) {
    dsOwner.setOwnerId(owner.owner.toString());
    dsOwner.setOwnerIdType(owner.ownerType.name());
    dsOwner.setOwnerType(owner.ownerCategory.name());
    dsOwner.setOwnerSource(owner.ownershipProvider.name());

    if (owner.ownerType == OwnerType.USER) {
      dsOwner.setAppId(300);
      dsOwner.setIsGroup("N");
      dsOwner.setNamespace("urn:li:corpuser");
    } else if (owner.ownerType == OwnerType.GROUP || owner.ownerType == OwnerType.SERVICE) {
      dsOwner.setAppId(301);
      dsOwner.setIsGroup("Y");
      dsOwner.setNamespace("urn:li:corpGroup");
    } else {
      dsOwner.setAppId(0);
      dsOwner.setIsGroup("N");
    }

    // TODO: check LDAP table to update is_active
    dsOwner.setIsActive("Y");

    dsOwner.setSourceTime(sourceTime);
  }

  /**
   * Find the updated list of owners, and the list of owners that don't exist anymore.
   * @param originalList List<DsOwner>
   * @param owners List<OwnerInfo>
   * @param datasetId int
   * @param datasetUrn String
   * @param sourceTime int epoch second
   * @return [ updated list , removed list of owners]
   */
  public List<List<DsOwner>> diffOwnerList(List<DsOwner> originalList, List<OwnerInfo> owners, int datasetId,
      String datasetUrn, int sourceTime) {

    List<DsOwner> updatedOwners = new ArrayList<>();

    int ownerCount = 0;
    for (OwnerInfo ownerInfo : owners) {
      ownerCount++;

      String owner = ownerInfo.owner.toString();
      // find and update existing owners
      for (DsOwner dsOwner : originalList) {
        if (owner.equalsIgnoreCase(dsOwner.getOwnerId())) {
          dsOwner.setDatasetUrn(datasetUrn);
          fillDsOwnerByOwnerInfo(ownerInfo, dsOwner, sourceTime);

          updatedOwners.add(dsOwner);
          break;
        }
      }

      // if field not exist, add a new field
      if (updatedOwners.size() < ownerCount) {
        DsOwner dsOwner = new DsOwner();
        dsOwner.setDatasetId(datasetId);
        dsOwner.setDatasetUrn(datasetUrn);
        fillDsOwnerByOwnerInfo(ownerInfo, dsOwner, sourceTime);

        updatedOwners.add(dsOwner);
      }
    }

    // remove non-exist owners, the merge updated owners
    List<DsOwner> removedOwners = new ArrayList<>(originalList);
    removedOwners.removeAll(updatedOwners);

    return Arrays.asList(updatedOwners, removedOwners);
  }
}
