package io.datahubproject.wiring;

import java.util.List;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.process.CommandLineArgumentProvider;

/**
 * Tells the test-side recorder ({@code :wiring-coverage}) where to write, and declares that
 * directory as a {@code Test} output so it is restored with the test task from the build cache.
 */
public abstract class WiringCoverageRecorderArgs implements CommandLineArgumentProvider {

  @OutputDirectory
  public abstract DirectoryProperty getDir();

  @Override
  public Iterable<String> asArguments() {
    return List.of("-Ddatahub.wiring.coverage.dir=" + getDir().get().getAsFile().getAbsolutePath());
  }
}
