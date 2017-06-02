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

import java.lang.reflect.Constructor;
import java.util.Properties;
import metadata.etl.EtlJob;
import metadata.etl.dataset.espresso.EspressoMetadataEtl;
import metadata.etl.dataset.hdfs.HdfsMetadataEtl;
import metadata.etl.dataset.hive.HiveMetadataEtl;
import metadata.etl.dataset.kafka.KafkaMetadataEtl;
import metadata.etl.dataset.oracle.OracleMetadataEtl;
import metadata.etl.dataset.teradata.TeradataMetadataEtl;
import metadata.etl.dataset.voldemort.VoldemortMetadataEtl;
import metadata.etl.elasticsearch.ElasticSearchBuildIndexETL;
import metadata.etl.git.CodeSearchMetadataEtl;
import metadata.etl.git.GitMetadataEtl;
import metadata.etl.git.MultiproductMetadataEtl;
import metadata.etl.ldap.LdapEtl;
import metadata.etl.lineage.AzLineageMetadataEtl;
import metadata.etl.lineage.appworx.AppworxLineageEtl;
import metadata.etl.metadata.DatasetDescriptionEtl;
import metadata.etl.ownership.DaliViewOwnerEtl;
import metadata.etl.ownership.DatasetOwnerEtl;
import metadata.etl.scheduler.appworx.AppworxExecEtl;
import metadata.etl.scheduler.azkaban.AzkabanExecEtl;
import metadata.etl.scheduler.oozie.OozieExecEtl;
import metadata.etl.security.DatasetConfidentialFieldEtl;


/**
 * Created by zechen on 10/21/15.
 */
public class EtlJobFactory {

  public static EtlJob getEtlJob(String etlClassName, Long whExecId, Properties properties)
      throws Exception {
    Class etlClass = Class.forName(etlClassName);
    Constructor<?> ctor = etlClass.getConstructor(int.class, long.class, Properties.class);
    return (EtlJob) ctor.newInstance(0, whExecId, properties);
  }
}
