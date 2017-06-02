package metadata.etl.models;

import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EtlJobFactoryTest {

  @Test
  public void testGetEtlJob() throws Exception {
    Properties properties = new Properties();
    DummyEtlJob job = (DummyEtlJob) EtlJobFactory.getEtlJob(DummyEtlJob.class.getCanonicalName(), 2L, properties);

    Assert.assertEquals(job.whExecId, 2L);
    Assert.assertEquals(job.properties, properties);
  }
}
