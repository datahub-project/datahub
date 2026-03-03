package io.datahubproject.models.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TestHelper {

  private static final String TEST_RESOURCES_DIR = "src/test/resources";
  //  private static final String TEMP_OUTPUT_DIR = "build/test-outputs";
  private static final String PYTHON_SCRIPT = "scripts/container_key_guid_generator.py";
  private static final String VENV_PATH =
      "../../../metadata-ingestion/venv"; // Adjust this path to your venv location

  public static void setup() {
    // Create output directory if it doesn't exist
    //    new File(TEMP_OUTPUT_DIR).mkdirs();

    // Verify venv exists
    if (!new File(VENV_PATH).exists()) {
      throw new RuntimeException("Virtual environment not found at " + VENV_PATH);
    }
  }

  private static ProcessBuilder createPythonProcessBuilder(String... args) {
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

    return pb;
  }

  /**
   * Executes the container key GUID generator Python script with the given parameters
   *
   * @param containerType The type of container (e.g., "database")
   * @param parameters Map of parameter key-value pairs
   * @return The generated URN as a string
   * @throws RuntimeException if the Python script execution fails
   */
  public static String generateContainerKeyGuid(
      String containerType, Map<String, String> parameters) {
    List<String> command = new ArrayList<>();
    command.add(PYTHON_SCRIPT);
    command.add("--container-type");
    command.add(containerType);

    // Add each parameter as -p key=value
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      command.add("-p");
      command.add(entry.getKey() + "=" + entry.getValue());
    }

    try {
      System.out.println("Executing Python script: " + String.join(" ", command));
      String[] commandArray = command.toArray(new String[0]);

      Process process = createPythonProcessBuilder(commandArray).start();

      // Read the output
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = reader.readLine();

      // Wait for the process to complete
      int exitCode = process.waitFor();

      if (exitCode != 0) {
        // Read error stream if the script failed
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()));
        StringBuilder errorMessage = new StringBuilder();
        String errorLine;
        while ((errorLine = errorReader.readLine()) != null) {
          errorMessage.append(errorLine).append("\n");
        }
        throw new RuntimeException(
            "Python script failed with exit code " + exitCode + ": " + errorMessage);
      }

      return line != null ? line.trim() : "";
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute Python script", e);
    }
  }
}
