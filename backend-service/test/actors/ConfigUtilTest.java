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

import com.google.common.io.Files;
import metadata.etl.models.EtlJobName;
import org.junit.Test;
import wherehows.common.Constant;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import static org.junit.Assert.assertThat;

public class ConfigUtilTest {

  @Test
  public void shouldGenerateEtlJobDefaultCommand() {
    // when:
    String command = ConfigUtil.generateCommand(0L, "", new Properties());

    // then:
    assertThat(command)
            .startsWith("java -cp ")
            .contains(" -Dconfig=/var/tmp/wherehows/exec/0.properties ")
            .endsWith(" metadata.etl.Launcher");
  }

  @Test
  public void shouldGenerateEtlJobCommandWithConfiguredDirectory() {
    final String applicationDirectory = "./temporary-directory";

    // given:
    Properties etlJobProperties = new Properties();
    etlJobProperties.put(Constant.WH_APP_FOLDER_KEY, applicationDirectory);

    // when:
    String command = ConfigUtil.generateCommand(1L, "", etlJobProperties);

    // then:
    assertThat(command)
            .startsWith("java -cp ")
            .contains(" -Dconfig=" + applicationDirectory + "/exec/1.properties ")
            .endsWith(" metadata.etl.Launcher");
  }

  @Test
  public void shouldGeneratePropertiesWithValues() throws IOException {
    final long whEtlExecId = System.currentTimeMillis();
    final Properties etlJobProperties = new Properties();
    etlJobProperties.put("p1", "v1");
    etlJobProperties.put("p2", "v2");
    etlJobProperties.put("p3", "v3");

    final File propertiesFile = createTemporaryPropertiesFile(whEtlExecId, etlJobProperties);

    // when:
    final EtlJobName etlJobName = EtlJobName.valueOf("HIVE_DATASET_METADATA_ETL");
    ConfigUtil.generateProperties(etlJobName, 2, whEtlExecId, etlJobProperties);

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
    final File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    final String tempDirPath = tempDir.getAbsolutePath();

    etlJobProperties.put(Constant.WH_APP_FOLDER_KEY, tempDirPath);

    return new File(tempDirPath + "/exec", whEtlExecId + ".properties");
  }
}
