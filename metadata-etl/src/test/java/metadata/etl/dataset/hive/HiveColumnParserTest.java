package metadata.etl.dataset.hive;

import java.io.InputStream;
import metadata.etl.LaunchJython;
import org.python.util.PythonInterpreter;
import org.testng.annotations.Test;


/**
 * Created by zsun on 3/9/16.
 */
public class HiveColumnParserTest {

  @Test
  public void hiveColumnParserTest() {

    LaunchJython l = new LaunchJython();
    PythonInterpreter interpreter = l.setUp();

    ClassLoader classLoader = getClass().getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("jython-test/HiveColumnParserTest.py");
    interpreter.execfile(inputStream);

  }
}
