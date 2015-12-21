package metadata.etl.dataset.hive;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


/**
 * Created by zsun on 11/13/15.
 */
public class HiveTest {
  HiveMetadataEtl hm;

  @BeforeTest
  public void setUp()
    throws Exception {
    hm = new HiveMetadataEtl(0, 0L);
  }

  @Test
  public void extractTest()
    throws Exception {
    hm.extract();
    // check the json file
  }

  @Test
  public void transformTest()
      throws Exception {
    hm.transform();
    // check the csv file
  }

  @Test
  public void loadTest()
      throws Exception {
    hm.load();
    // check in database
  }

  @Test
  public void runTest()
    throws Exception {
    extractTest();
    transformTest();
    loadTest();
  }

}
