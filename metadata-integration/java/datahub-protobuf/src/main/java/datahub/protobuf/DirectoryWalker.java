package datahub.protobuf;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.stream.Stream;

public class DirectoryWalker {
  private final Path rootDirectory;
  private final PathMatcher includeMatcher;
  private final ArrayList<PathMatcher> excludeMatchers;

  public DirectoryWalker(String directory, String[] excludePatterns) {
    this.rootDirectory = Path.of(directory);
    this.includeMatcher = FileSystems.getDefault().getPathMatcher("glob:**/*.proto");
    this.excludeMatchers = new ArrayList<>();
    if (excludePatterns != null) {
      for (String excludePattern : excludePatterns) {
        this.excludeMatchers.add(FileSystems.getDefault().getPathMatcher("glob:" + excludePattern));
      }
    }

  }

  public Stream<Path> walkFiles() throws IOException {
    final Path baseDir = this.rootDirectory;
    final ArrayList<Path> files = new ArrayList<>();
    Files.walkFileTree(this.rootDirectory, new FileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        boolean excluded = false;
        Path relativePath = baseDir.relativize(file);
        if (!includeMatcher.matches(relativePath)) {
          excluded = true;
        } else {
          for (PathMatcher matcher : excludeMatchers) {
            if (matcher.matches(relativePath)) {
              excluded = true;
            }
          }
        }

        if (!excluded) {
          files.add(file);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }
    });

    return files.stream();
  }


}
