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

import java.util.Properties;
import metadata.etl.EtlJob;
import metadata.etl.dataset.hdfs.HdfsMetadataEtl;
import metadata.etl.dataset.hive.HiveMetadataEtl;
import metadata.etl.dataset.oracle.OracleMetadataEtl;
import metadata.etl.dataset.teradata.TeradataMetadataEtl;
import metadata.etl.elasticsearch.ElasticSearchBuildIndexETL;
import metadata.etl.git.GitMetadataEtl;
import metadata.etl.git.MultiproductMetadataEtl;
import metadata.etl.lineage.AzLineageMetadataEtl;
import metadata.etl.ownership.DatasetOwnerEtl;
import metadata.etl.ldap.LdapEtl;
import metadata.etl.scheduler.azkaban.AzkabanExecEtl;
import metadata.etl.scheduler.oozie.OozieExecEtl;
import metadata.etl.models.EtlJobName;
import metadata.etl.git.CodeSearchMetadataEtl;


/**
 * Created by zechen on 10/21/15.
 */
public class EtlJobFactory {

  public static EtlJob getEtlJob(EtlJobName etlJobName, Integer refId, Long whExecId, Properties properties) {
    switch (etlJobName) {
      case AZKABAN_EXECUTION_METADATA_ETL:
        return new AzkabanExecEtl(refId, whExecId, properties);
      case OOZIE_EXECUTION_METADATA_ETL:
        return new OozieExecEtl(refId, whExecId, properties);
      case HADOOP_DATASET_METADATA_ETL:
        return new HdfsMetadataEtl(refId, whExecId, properties);
      case TERADATA_DATASET_METADATA_ETL:
        return new TeradataMetadataEtl(refId, whExecId, properties);
      case AZKABAN_LINEAGE_METADATA_ETL:
        return new AzLineageMetadataEtl(refId, whExecId, properties);
      case HADOOP_DATASET_OWNER_ETL:
        return new DatasetOwnerEtl(refId, whExecId, properties);
      case LDAP_USER_ETL:
        return new LdapEtl(refId, whExecId, properties);
      case GIT_MEDATA_ETL:
        return new GitMetadataEtl(refId, whExecId, properties);
      case HIVE_DATASET_METADATA_ETL:
        return new HiveMetadataEtl(refId, whExecId, properties);
      case ELASTICSEARCH_EXECUTION_INDEX_ETL:
        return new ElasticSearchBuildIndexETL(refId, whExecId, properties);
      case TREEBUILDER_EXECUTION_DATASET_ETL:
        return new ElasticSearchBuildIndexETL(refId, whExecId, properties);
      case ORACLE_DATASET_METADATA_ETL:
        return new OracleMetadataEtl(refId, whExecId, properties);
      case PRODUCT_REPO_METADATA_ETL:
        return new MultiproductMetadataEtl(refId, whExecId, properties);
      case DATABASE_SCM_METADATA_ETL:
        return new CodeSearchMetadataEtl(refId, whExecId, properties);
      default:
        throw new UnsupportedOperationException("Unsupported job type: " + etlJobName);
    }
  }
}
