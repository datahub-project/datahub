package metadata.etl.dataset.hive;

import java.io.InputStream;
import metadata.etl.LaunchJython;
import org.python.util.PythonInterpreter;
import org.testng.annotations.Test;


public class AvroColumnParserTest {

  @Test
  public void avroTest() {

    LaunchJython l = new LaunchJython();
    PythonInterpreter interpreter = l.setUp();

    ClassLoader classLoader = getClass().getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("jython-test/AvroColumnParserTest.py");
    interpreter.execfile(inputStream);

  }
}
