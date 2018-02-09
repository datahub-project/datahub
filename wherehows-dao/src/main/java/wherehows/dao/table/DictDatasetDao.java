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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.events.metadata.Capacity;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DatasetProperty;
import com.linkedin.events.metadata.DatasetSchema;
import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.PartitionSpecification;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import wherehows.models.table.DictDataset;

import static wherehows.util.UrnUtil.*;


@Slf4j
public class DictDatasetDao extends BaseDao {

  public DictDatasetDao(@Nonnull EntityManagerFactory factory) {
    super(factory);
  }

  private static final String SET_DATASET_DEPRECATION =
      "UPDATE DictDataset SET isDeprecated = :deprecated WHERE id = :datasetId";

  private static final String SET_DATASET_ACTIVE =
      "UPDATE DictDataset SET isActive = :active WHERE urn = :datasetUrn";

  public DictDataset findByUrn(@Nonnull String urn) {
    return findBy(DictDataset.class, "urn", urn);
  }

  public DictDataset findById(int datasetId) {
    return findBy(DictDataset.class, "id", datasetId);
  }

  /**
   * Insert or update dict dataset table given information from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param auditStamp ChangeAuditStamp
   * @param property DatasetProperty
   * @param schema DatasetSchema
   * @param deployments List<DeploymentDetail>
   * @param tags List<String>
   * @param capacities List<Capacity>
   * @return dataset id
   * @throws Exception
   */
  @Nullable
  public DictDataset insertUpdateDataset(@Nonnull DatasetIdentifier identifier, @Nonnull ChangeAuditStamp auditStamp,
      @Nullable DatasetProperty property, @Nullable DatasetSchema schema, @Nullable List<DeploymentDetail> deployments,
      @Nullable List<String> tags, @Nullable List<Capacity> capacities, @Nullable PartitionSpecification partitions)
      throws Exception {

    String urn = toWhDatasetUrn(identifier);

    // find dataset if exist
    DictDataset dataset = null;
    try {
      dataset = findBy(DictDataset.class, "urn", urn);
    } catch (Exception e) {
      log.debug("Can't find dataset " + urn, e.toString());
    }
    // if not found, create new entity
    if (dataset == null) {
      dataset = new DictDataset();
    }

    // fill in information
    fillDictDataset(dataset, urn, auditStamp, property, schema, deployments, tags, capacities, partitions);

    // merge into table
    return (DictDataset) update(dataset);
  }

  /**
   * Fill in DictDataset information
   * @param ds DictDataset
   * @param urn String
   * @param auditStamp ChangeAuditStamp
   * @param property DatasetProperty
   * @param schema DatasetSchema
   * @param deployments List<DeploymentDetail>
   * @param tags List<String>
   * @param capacities List<Capacity>
   * @param partitions PartitionSpecification
   * @throws IOException
   */
  public void fillDictDataset(@Nonnull DictDataset ds, @Nonnull String urn, @Nonnull ChangeAuditStamp auditStamp,
      @Nullable DatasetProperty property, @Nullable DatasetSchema schema, @Nullable List<DeploymentDetail> deployments,
      @Nullable List<String> tags, List<Capacity> capacities, @Nullable PartitionSpecification partitions)
      throws IOException {

    ObjectMapper mapper = new ObjectMapper();

    if (ds.getUrn() == null) {
      ds.setUrn(urn);
    }

    String[] urnParts = parseWhDatasetUrn(urn);
    ds.setDatasetType(urnParts[0]);
    ds.setSource(urnParts[0]);
    ds.setLocationPrefix(urnParts[1]);
    ds.setParentName(urnParts[2]);
    ds.setName(urnParts[3]);

    // put extra information into properties
    Map<String, Object> propertiesMap;
    try {
      propertiesMap = mapper.readValue(ds.getProperties(), new TypeReference<Map<String, Object>>() {
      });
    } catch (Exception ex) {
      propertiesMap = new HashMap<>();
    }

    if (deployments != null) {
      propertiesMap.put("deployment", deployments.toString());
    }
    if (tags != null) {
      propertiesMap.put("tag", tags);
    }
    if (capacities != null) {
      propertiesMap.put("capacity", capacities.toString());
    }
    if (partitions != null) {
      propertiesMap.put("partition", partitions.toString());
    }
    if (property != null) {
      propertiesMap.put("property", property.toString());
      if (property.storageType != null) {
        ds.setStorageType(property.storageType.name());
      }
    }
    ds.setProperties(mapper.writeValueAsString(propertiesMap));

    ds.setIsActive(true); // delete?

    if (schema != null) {
      ds.setSchema(coalesce(toStringOrNull(schema.rawSchema.content), ds.getSchema()));
      ds.setSchemaType(coalesce(schema.rawSchema.format.name(), ds.getSchemaType()));

      if (schema.fieldSchema != null) {
        ds.setFields(schema.fieldSchema.toString());
      }

      // if schema is not in the MCE, will not update the source section of the dataset
      int sourceTime = (int) (auditStamp.time / 1000);
      if (ds.getSourceCreatedTime() == null) {
        ds.setSourceCreatedTime(sourceTime);
      }
      ds.setSourceModifiedTime(sourceTime);
    }
  }

  /**
   * Set deprecation status of a dataset.
   * @param datasetId int
   * @param datasetUrn String
   * @param isDeprecated boolean
   * @param deprecationNote String
   * @param user String
   * @throws Exception
   */
  public void setDatasetDeprecation(int datasetId, @Nonnull String datasetUrn, boolean isDeprecated,
      @Nullable String deprecationNote, @Nonnull String user) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetId", datasetId);
    params.put("deprecated", isDeprecated);

    executeUpdate(SET_DATASET_DEPRECATION, params);
  }

  public void setDatasetDeprecation(@Nonnull String datasetUrn, boolean isDeprecated,
      @Nullable String deprecationNote, @Nonnull String user) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Set active/removed status of a dataset.
   * @param identifier DatasetIdentifier
   * @param isRemoved boolean
   * @param auditStamp ChangeAuditStamp
   * @throws Exception
   */
  public void setDatasetRemoved(@Nonnull DatasetIdentifier identifier, boolean isRemoved,
      @Nonnull ChangeAuditStamp auditStamp) throws Exception {

    String urn = toWhDatasetUrn(identifier);

    Map<String, Object> params = new HashMap<>();
    params.put("datasetUrn", urn);
    params.put("active", !isRemoved);

    executeUpdate(SET_DATASET_ACTIVE, params);
  }
}
