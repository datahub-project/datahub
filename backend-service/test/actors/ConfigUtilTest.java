package actors;

import com.google.common.io.Files;
import metadata.etl.models.EtlJobName;
import org.junit.Test;
import wherehows.common.Constant;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import static org.fest.assertions.Assertions.assertThat;

public class ConfigUtilTest {

  @Test
  public void shouldGenerateEtlJobDefaultCommand() {
    // when:
    String command = ConfigUtil.generateCommand(0L, "", new Properties());

    // then:
    assertThat(command.startsWith("java -cp ")).isTrue();
    assertThat(command.endsWith(" -Dconfig=/var/tmp/wherehows/exec/0.properties metadata.etl.Launcher")).isTrue();
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
    assertThat(command.startsWith("java -cp ")).isTrue();
    assertThat(command.endsWith(" -Dconfig=" + applicationDirectory + "/exec/1.properties metadata.etl.Launcher")).isTrue();
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
    assertThat(content).contains("p1=v1");
    assertThat(content).contains("p2=v2");
    assertThat(content).contains("p3=v3");
  }

  @Test
  public void shouldGeneratePropertiesFileAndDeleteIt() throws IOException {
    final long whEtlExecId = System.currentTimeMillis();
    final Properties etlJobProperties = new Properties();

    final File propertiesFile = createTemporaryPropertiesFile(whEtlExecId, etlJobProperties);

    // expect:
    assertThat(propertiesFile.exists()).isFalse();

    // when:
    final EtlJobName etlJobName = EtlJobName.valueOf("AZKABAN_EXECUTION_METADATA_ETL");
    ConfigUtil.generateProperties(etlJobName, 2, whEtlExecId, etlJobProperties);

    // then:
    assertThat(propertiesFile.exists()).isTrue();

    // when:
    ConfigUtil.deletePropertiesFile(etlJobProperties, whEtlExecId);

    // then:
    assertThat(propertiesFile.exists()).isFalse();
  }

  private File createTemporaryPropertiesFile(long whEtlExecId, Properties etlJobProperties) {
    final File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    final String tempDirPath = tempDir.getAbsolutePath();

    etlJobProperties.put(Constant.WH_APP_FOLDER_KEY, tempDirPath);

    return new File(tempDirPath + "/exec", whEtlExecId + ".properties");
  }
}
