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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.models.view.DatasetOwner;


public class OwnerViewDao extends BaseViewDao {

  public OwnerViewDao(EntityManagerFactory factory) {
    super(factory);
  }

  private final static String GET_DATASET_OWNERS_BY_ID =
      "SELECT o.owner_id, u.display_name, o.sort_id, o.owner_type, o.namespace, o.owner_id_type, o.owner_source, "
          + "o.owner_sub_type, o.confirmed_by, u.email, u.is_active, is_group, o.modified_time "
          + "FROM dataset_owner o "
          + "LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) "
          + "WHERE o.dataset_urn = :datasetUrn and (o.is_deleted is null OR o.is_deleted != 'Y') ORDER BY o.sort_id";


  /**
   * Get dataset owner list by WH dataset URN
   * @param datasetUrn String
   * @return List of DatasetOwner
   */
  @Nullable
  public List<DatasetOwner> getDatasetOwnersByUrn(@Nonnull String datasetUrn) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetUrn", datasetUrn);

    List<DatasetOwner> owners = getEntityListBy(GET_DATASET_OWNERS_BY_ID, DatasetOwner.class, params);
    for (DatasetOwner owner : owners) {
      owner.setModifiedTime(owner.getModifiedTime() * 1000);
    }
    return owners;
  }

  @Nullable
  public List<DatasetOwner> getDatasetOwners(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
