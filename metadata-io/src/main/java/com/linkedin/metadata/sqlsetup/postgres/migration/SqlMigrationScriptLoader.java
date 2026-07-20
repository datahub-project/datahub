package com.linkedin.metadata.sqlsetup.postgres.migration;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/** Discovers VERSIONED and REPEATABLE SQL scripts on the classpath. */
final class SqlMigrationScriptLoader {

  private static final Pattern VERSIONED =
      Pattern.compile("^V(\\d+)__(.+)\\.sql$", Pattern.CASE_INSENSITIVE);
  private static final Pattern REPEATABLE =
      Pattern.compile("^R__(.+)\\.sql$", Pattern.CASE_INSENSITIVE);

  private SqlMigrationScriptLoader() {}

  @Nonnull
  static List<SqlMigrationScript> discover(
      @Nonnull ClassLoader classLoader, @Nonnull String location) throws IOException {
    String dir = location.endsWith("/") ? location : location + "/";
    List<String> fileNames = listSqlFileNames(classLoader, dir);
    List<SqlMigrationScript> scripts = new ArrayList<>();
    int maxVersion = 0;
    for (String name : fileNames) {
      Matcher v = VERSIONED.matcher(name);
      if (v.matches()) {
        int rank = Integer.parseInt(v.group(1));
        maxVersion = Math.max(maxVersion, rank);
        scripts.add(
            new SqlMigrationScript(
                "V" + String.format("%03d", rank),
                v.group(2),
                SqlMigrationType.VERSIONED,
                name,
                dir + name,
                rank));
        continue;
      }
      Matcher r = REPEATABLE.matcher(name);
      if (r.matches()) {
        scripts.add(
            new SqlMigrationScript(
                "R__" + r.group(1),
                r.group(1),
                SqlMigrationType.REPEATABLE,
                name,
                dir + name,
                1_000_000 + scripts.size()));
      }
    }
    scripts.sort(
        Comparator.comparing(SqlMigrationScript::getType)
            .thenComparingInt(SqlMigrationScript::getVersionRank));
    int repeatableBase = maxVersion + 1;
    List<SqlMigrationScript> reordered = new ArrayList<>();
    int repeatableIndex = 0;
    for (SqlMigrationScript script : scripts) {
      if (script.getType() == SqlMigrationType.REPEATABLE) {
        reordered.add(
            new SqlMigrationScript(
                script.getVersion(),
                script.getDescription(),
                script.getType(),
                script.getScriptName(),
                script.getClasspathLocation(),
                repeatableBase + repeatableIndex++));
      } else {
        reordered.add(script);
      }
    }
    return reordered;
  }

  @Nonnull
  private static List<String> listSqlFileNames(
      @Nonnull ClassLoader classLoader, @Nonnull String dir) throws IOException {
    Enumeration<URL> urls = classLoader.getResources(dir);
    List<String> names = new ArrayList<>();
    while (urls.hasMoreElements()) {
      URL url = urls.nextElement();
      if ("file".equals(url.getProtocol())) {
        try {
          Path path = Path.of(url.toURI());
          if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
              stream
                  .filter(p -> p.getFileName().toString().endsWith(".sql"))
                  .map(p -> p.getFileName().toString())
                  .forEach(names::add);
            }
          }
        } catch (URISyntaxException e) {
          throw new IOException("Invalid resource URL: " + url, e);
        }
      } else if ("jar".equals(url.getProtocol())) {
        JarURLConnection conn = (JarURLConnection) url.openConnection();
        try (JarFile jar = conn.getJarFile()) {
          String prefix = dir.endsWith("/") ? dir : dir + "/";
          Enumeration<JarEntry> entries = jar.entries();
          while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String entryName = entry.getName();
            if (entryName.startsWith(prefix)
                && entryName.endsWith(".sql")
                && !entry.isDirectory()) {
              names.add(entryName.substring(prefix.length()));
            }
          }
        }
      }
    }
    return names.stream().distinct().sorted().toList();
  }
}
