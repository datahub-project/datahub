package io.datahubproject.schematron;

import static org.testng.Assert.assertEquals;

import io.datahubproject.schematron.cli.SchemaTron;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class SchemaTranslatorTest {
  private static final String TEST_RESOURCES_DIR = "src/test/resources";
  private static final String TEMP_OUTPUT_DIR = "build/test-outputs";
  private static final String PYTHON_SCRIPT = "scripts/avro_schema_to_mce.py";
  private static final String DIFF_SCRIPT = "scripts/mce_diff.py";
  private static final String VENV_PATH =
      "../../../../metadata-ingestion/venv"; // Adjust this path to your venv location

  @BeforeClass
  public static void setup() {
    // Create output directory if it doesn't exist
    new File(TEMP_OUTPUT_DIR).mkdirs();

    // Verify venv exists
    if (!new File(VENV_PATH).exists()) {
      throw new RuntimeException("Virtual environment not found at " + VENV_PATH);
    }
  }

  @DataProvider(name = "schemaFiles")
  public Object[][] getSchemaFiles() throws Exception {
    List<Path> schemaFiles =
        Files.walk(Paths.get(TEST_RESOURCES_DIR))
            .filter(path -> path.toString().endsWith(".avsc"))
            .collect(Collectors.toList());

    Object[][] testData = new Object[schemaFiles.size()][1];
    for (int i = 0; i < schemaFiles.size(); i++) {
      testData[i][0] = schemaFiles.get(i);
    }
    return testData;
  }

  @Test(dataProvider = "schemaFiles")
  public void testSchemaTranslations(Path schemaFile) throws Exception {
    compareTranslations(schemaFile);
  }

  private ProcessBuilder createPythonProcessBuilder(String... args) {
    ProcessBuilder pb;
    String os = System.getProperty("os.name").toLowerCase();

    if (os.contains("windows")) {
      // Windows paths
      String pythonPath = Paths.get(VENV_PATH, "Scripts", "python").toString();
      pb =
          new ProcessBuilder(
              Stream.concat(Stream.of(pythonPath), Stream.of(args)).toArray(String[]::new));
    } else {
      // Unix-like paths
      String pythonPath = Paths.get(VENV_PATH, "bin", "python").toString();
      pb =
          new ProcessBuilder(
              Stream.concat(Stream.of(pythonPath), Stream.of(args)).toArray(String[]::new));
    }

    // Add virtual environment to PYTHONPATH
    Map<String, String> env = pb.environment();
    String sitePkgPath =
        Paths.get(
                VENV_PATH,
                os.contains("windows") ? "Lib/site-packages" : "lib/python3.x/site-packages")
            .toString();

    String pythonPath = env.getOrDefault("PYTHONPATH", "");
    env.put("PYTHONPATH", pythonPath + File.pathSeparator + sitePkgPath);

    return pb.inheritIO();
  }

  private void compareTranslations(Path schemaFile) throws Exception {
    String baseName = schemaFile.getFileName().toString().replace(".avsc", "");
    String javaOutput = TEMP_OUTPUT_DIR + "/" + baseName + "_java.json";
    String pythonOutput = TEMP_OUTPUT_DIR + "/" + baseName + "_python.json";
    String diffFile = schemaFile.getParent().toString() + "/diffs/" + baseName + "_diff.json";

    // Test if diffFile exists
    File diff = new File(diffFile);
    if (!diff.exists()) {
      diffFile = null;
    }

    // Run Python translator
    Process pythonProcess =
        createPythonProcessBuilder(
                PYTHON_SCRIPT,
                "--platform",
                "datahub",
                "--input-file",
                schemaFile.toString(),
                "--output-file",
                pythonOutput)
            .inheritIO()
            .start();

    int pythonExitCode = pythonProcess.waitFor();
    assertEquals(pythonExitCode, 0, "Python translation failed");

    // Run Java translator directly using SchemaTron
    SchemaTron schemaTron = new SchemaTron();
    int javaExitCode =
        new CommandLine(schemaTron)
            .execute(
                "-i",
                schemaFile.toAbsolutePath().toString(),
                "--sink",
                "file",
                "--output-file",
                javaOutput,
                "--platform",
                "datahub");

    assertEquals(javaExitCode, 0, "Java translation failed");

    // Compare outputs
    // if diffFile is not provided, we just compare the outputs
    ProcessBuilder diffProcessBuilder;
    if (diffFile == null) {
      diffProcessBuilder = createPythonProcessBuilder(DIFF_SCRIPT, pythonOutput, javaOutput);
    } else {
      diffProcessBuilder =
          createPythonProcessBuilder(
              DIFF_SCRIPT, pythonOutput, javaOutput, "--golden-diff-file", diffFile);
    }

    Process diffProcess = diffProcessBuilder.inheritIO().start();

    int diffExitCode = diffProcess.waitFor();
    assertEquals(diffExitCode, 0, "Outputs differ for " + schemaFile.getFileName());
  }
}
