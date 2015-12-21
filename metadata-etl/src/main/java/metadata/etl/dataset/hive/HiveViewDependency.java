package metadata.etl.dataset.hive;

import java.util.TreeSet;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by zsun on 12/14/15.
 */
public class HiveViewDependency {
  final Logger logger = LoggerFactory.getLogger(getClass());
  LineageInfo lineageInfoTool;

  public HiveViewDependency() {
    lineageInfoTool = new LineageInfo();
  }
  public String[] getViewDependency(String hiveQl) {
    try {
      lineageInfoTool.getLineageInfo(hiveQl);
      TreeSet<String> inputs = lineageInfoTool.getInputTableList();
      return inputs.toArray(new String[inputs.size()]);
    } catch (Exception e) {
      logger.error("Sql statements : \n" + hiveQl + "\n parse ERROR, will return an empty String array");
      logger.error(String.valueOf(e.getCause()));
      return new String[]{};
    }
  }
}
