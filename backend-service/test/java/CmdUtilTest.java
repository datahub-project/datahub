

import java.util.Properties;
import metadata.etl.models.EtlJobName;
import org.testng.Assert;
import org.testng.annotations.Test;
import actors.CmdUtil;


public class CmdUtilTest {

  @Test
  public void testgenerateCMD(){
    EtlJobName etlJobName = EtlJobName.valueOf("AZKABAN_EXECUTION_METADATA_ETL");
    Properties prop = new Properties();
    prop.put("p1", "v1");
    prop.put("p2", "v2");
    prop.put("p3", "v3");

    String cmd = CmdUtil.generateCMD(etlJobName, 0, 0L, prop);

    // class path is dynamic, can't predefine
    Assert.assertTrue(
        cmd.startsWith("java -Djob=AZKABAN_EXECUTION_METADATA_ETL -DrefId=0 -DwhEtlId=0 -Dp3=v3 -Dp2=v2 -Dp1=v1 -cp"));
  }

}