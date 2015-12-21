package metadata.etl.dataset.hive;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by zsun on 12/9/15.
 */
public class HiveViewDependencyParserTest {
  @Test
  public void parseTest()
      throws CommandNeedRetryException, SemanticException, ParseException {
    String hiveQl = "select t1.c2 from (select t2.column2 c2, t3.column3 from db1.table2 t2 join db2.table3 t3 on t2.x = t3.y) t1";
    HiveViewDependency hiveViewDependency = new HiveViewDependency();
    String[] result = hiveViewDependency.getViewDependency(hiveQl);
    String[] expctedResult = new String[]{"db1.table2", "db2.table3"};
    Assert.assertEquals(expctedResult, result);
  }
}
