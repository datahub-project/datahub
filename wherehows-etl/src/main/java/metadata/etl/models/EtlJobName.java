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
package metadata.etl.models;

/**
 * Created by zechen on 9/4/15.
 */
public enum EtlJobName {
  AZKABAN_EXECUTION_METADATA_ETL(EtlType.OPERATION, RefIdType.APP),
  APPWORX_EXECUTION_METADATA_ETL(EtlType.OPERATION, RefIdType.APP),
  OOZIE_EXECUTION_METADATA_ETL(EtlType.OPERATION, RefIdType.APP),
  HADOOP_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  TERADATA_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  AZKABAN_LINEAGE_METADATA_ETL(EtlType.LINEAGE, RefIdType.APP),
  APPWORX_LINEAGE_METADATA_ETL(EtlType.LINEAGE, RefIdType.APP),
  HADOOP_DATASET_OWNER_ETL(EtlType.OWNER, RefIdType.DB),
  LDAP_USER_ETL(EtlType.LDAP, RefIdType.APP),
  GIT_MEDATA_ETL(EtlType.VCS, RefIdType.APP),
  HIVE_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  ELASTICSEARCH_EXECUTION_INDEX_ETL(EtlType.OPERATION, RefIdType.APP),
  TREEBUILDER_EXECUTION_DATASET_ETL(EtlType.OPERATION, RefIdType.APP),
  ORACLE_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  PRODUCT_REPO_METADATA_ETL(EtlType.OPERATION, RefIdType.APP),
  KAFKA_CONSUMER_ETL(EtlType.OPERATION, RefIdType.DB),
  DATABASE_SCM_METADATA_ETL(EtlType.OPERATION, RefIdType.APP),
  DALI_VIEW_OWNER_ETL(EtlType.OWNER, RefIdType.DB),
  CONFIDENTIAL_FIELD_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  DATASET_DESCRIPTION_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  ESPRESSO_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  VOLDEMORT_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB),
  KAFKA_DATASET_METADATA_ETL(EtlType.DATASET, RefIdType.DB);

  EtlType etlType;
  RefIdType refIdType;

  EtlJobName(EtlType etlType, RefIdType refIdType) {
    this.etlType = etlType;
    this.refIdType = refIdType;
  }

  public EtlType getEtlType() {
    return etlType;
  }

  public RefIdType getRefIdType() {
    return refIdType;
  }

  public boolean affectDataset() {
    return this.getEtlType().equals(EtlType.DATASET);
  }

  public boolean affectFlow() {
    return this.getEtlType().equals(EtlType.OPERATION);
  }
}
