

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import metadata.etl.models.EtlJobName;
import org.testng.Assert;
import org.testng.annotations.Test;
import actors.ConfigUtil;
import wherehows.common.Constant;


public class ConfigUtilTest {

  @Test
  public void testgenerateCMD(){
    EtlJobName etlJobName = EtlJobName.valueOf("AZKABAN_EXECUTION_METADATA_ETL");
    Properties prop = new Properties();
    prop.put("p1", "v1");
    prop.put("p2", "v2");
    prop.put("p3", "v3");
    prop.put(Constant.WH_APP_FOLDER_KEY, "/var/tmp/wherehows");

    String cmd = ConfigUtil.generateCMD(0L, "");
    Assert.assertTrue(cmd.startsWith("java -cp "));
    Assert.assertTrue(cmd.endsWith(" -Dconfig=/var/tmp/wherehows/exec/0.properties metadata.etl.Launcher"));
    File configFile = new File("/var/tmp/wherehows/exec", "0.properties");
    Assert.assertTrue(!configFile.exists());
    try {
      ConfigUtil.generateProperties(etlJobName, 0, 0L, prop);
      Assert.assertTrue(configFile.exists());
      ConfigUtil.deletePropertiesFile(prop, 0L);
      Assert.assertTrue(!configFile.exists());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}