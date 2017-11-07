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
package actors;

import java.util.Properties;
import org.junit.Test;
import wherehows.common.Constant;

import static org.fest.assertions.api.Assertions.*;


public class ConfigUtilTest {

  @Test
  public void shouldGenerateEtlJobDefaultCommand() {
    // when:
    ProcessBuilder pb = ConfigUtil.buildProcess("java", "hdfs_metadata_etl", 0L, null, new Properties());

    // then:
    assertThat(pb.command()).contains("java", "-cp", System.getProperty("java.class.path"),
        "-Dconfig=/var/tmp/wherehows/0.properties", "-DCONTEXT=hdfs_metadata_etl",
        "-Dlogback.configurationFile=etl_logback.xml", "wherehows.common.jobs.Launcher");
  }

  @Test
  public void shouldGenerateEtlJobCommandWithConfiguredDirectory() {
    final String applicationDirectory = "./temp";

    // given:
    Properties etlJobProperties = new Properties();
    etlJobProperties.put(Constant.WH_APP_FOLDER_KEY, applicationDirectory);

    // when:
    ProcessBuilder pb = ConfigUtil.buildProcess("java", "ldap_user_etl", 1L, " -a -b  ", etlJobProperties);

    // then:
    assertThat(pb.command()).contains("java", "-a", "-b", "-cp", System.getProperty("java.class.path"),
        "-Dconfig=" + applicationDirectory + "/1.properties", "-DCONTEXT=ldap_user_etl",
        "-DLOG_DIR=" + applicationDirectory, "-Dlogback.configurationFile=etl_logback.xml",
        "wherehows.common.jobs.Launcher");
    assertThat(pb.redirectError().file().getPath().equals("./temp/LDAP_USER_ETL.stderr"));
    assertThat(pb.redirectOutput().file().getPath().equals("./temp/LDAP_USER_ETL.stdout"));
  }

  /*
  @Test
  public void shouldGeneratePropertiesWithValues() throws IOException {
    final long whEtlExecId = System.currentTimeMillis();
    final Properties etlJobProperties = new Properties();
    etlJobProperties.put("p1", "v1");
    etlJobProperties.put("p2", "v2");
    etlJobProperties.put("p3", "v3");

    final File propertiesFile = createTemporaryPropertiesFile(whEtlExecId, etlJobProperties);

    // when:
    ConfigUtil.generateProperties(EtlJobName.HIVE_DATASET_METADATA_ETL, 2, whEtlExecId, etlJobProperties);

    // then:
    final String content = Files.toString(propertiesFile, Charset.defaultCharset());
    assertThat(content)
            .contains("p1=v1")
            .contains("p2=v2")
            .contains("p3=v3");
  }

  @Test
  public void shouldGeneratePropertiesFileAndDeleteIt() throws IOException {
    final long whEtlExecId = System.currentTimeMillis();
    final Properties etlJobProperties = new Properties();

    final File propertiesFile = createTemporaryPropertiesFile(whEtlExecId, etlJobProperties);

    // expect:
    assertThat(propertiesFile).doesNotExist();

    // when:
    final EtlJobName etlJobName = EtlJobName.valueOf("AZKABAN_EXECUTION_METADATA_ETL");
    ConfigUtil.generateProperties(etlJobName, 2, whEtlExecId, etlJobProperties);

    // then:
    assertThat(propertiesFile).exists();

    // when:
    ConfigUtil.deletePropertiesFile(etlJobProperties, whEtlExecId);

    // then:
    assertThat(propertiesFile).doesNotExist();
  }

  private File createTemporaryPropertiesFile(long whEtlExecId, Properties etlJobProperties) {
    final File tempDir = new File(Files.createTempDir(), "non-exsiting-dir");
    tempDir.deleteOnExit();
    final String tempDirPath = tempDir.getAbsolutePath();

    etlJobProperties.put(Constant.WH_APP_FOLDER_KEY, tempDirPath);

    return new File(tempDirPath + "/exec", whEtlExecId + ".properties");
  }
  */
}
