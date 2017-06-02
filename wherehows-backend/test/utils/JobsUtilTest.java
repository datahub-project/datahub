package utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.fest.assertions.api.Assertions.*;


public class JobsUtilTest {

  private static String PROPERTIES = "var1=foo\n" + "var2=$foo\n" + "var3=${foo}";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testEnvVarResolution() throws IOException {
    environmentVariables.set("foo", "bar");
    Path path = createPropertiesFile(PROPERTIES);

    Properties properties = JobsUtil.getResolvedProperties(path);

    assertThat(properties).isNotEqualTo(null);
    assertThat(properties.get("var1")).isEqualTo("foo");
    assertThat(properties.get("var2")).isEqualTo("bar");
    assertThat(properties.get("var3")).isEqualTo("bar");

    Files.deleteIfExists(path);
  }

  private Path createPropertiesFile(String content) throws IOException {
    File propertyFile = File.createTempFile("temp", ".job");
    FileWriter writer = new FileWriter(propertyFile);
    writer.write(content);
    writer.close();
    return propertyFile.toPath();
  }
}
