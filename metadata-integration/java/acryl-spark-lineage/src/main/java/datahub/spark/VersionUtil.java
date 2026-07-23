package datahub.spark;

import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class VersionUtil {

  private static final String UNKNOWN_VERSION = "unknown";
  private static String cachedVersion = null;

  /** Get the version of the library from the JAR manifest */
  public static String getVersion() {
    if (cachedVersion != null) {
      return cachedVersion;
    }

    cachedVersion = getVersionFromManifest();
    return cachedVersion;
  }

  /** Get version from JAR manifest */
  private static String getVersionFromManifest() {
    try {
      // Get the class's package
      Package pkg = VersionUtil.class.getPackage();
      if (pkg != null && pkg.getImplementationVersion() != null) {
        return pkg.getImplementationVersion();
      }

      // Fallback: read manifest directly
      Class<?> clazz = VersionUtil.class;
      String className = clazz.getSimpleName() + ".class";
      String classPath = clazz.getResource(className).toString();

      if (classPath.startsWith("jar")) {
        String manifestPath =
            classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
        try (InputStream manifestStream = new java.net.URL(manifestPath).openStream()) {
          Manifest manifest = new Manifest(manifestStream);
          Attributes attributes = manifest.getMainAttributes();
          String version = attributes.getValue("Implementation-Version");
          if (version != null) {
            return version;
          }
        }
      }
    } catch (Exception e) {
      // Ignore and fall through to unknown
    }

    return UNKNOWN_VERSION;
  }

  /** Get detailed version information including Scala version */
  public static String getDetailedVersion() {
    try {
      Class<?> clazz = VersionUtil.class;
      String className = clazz.getSimpleName() + ".class";
      String classPath = clazz.getResource(className).toString();

      if (classPath.startsWith("jar")) {
        String manifestPath =
            classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
        try (InputStream manifestStream = new java.net.URL(manifestPath).openStream()) {
          Manifest manifest = new Manifest(manifestStream);
          Attributes attributes = manifest.getMainAttributes();

          String version = attributes.getValue("Implementation-Version");
          String scalaVersion = attributes.getValue("Scala-Version");
          String builtBy = attributes.getValue("Built-By");
          String builtDate = attributes.getValue("Built-Date");

          StringBuilder info = new StringBuilder();
          info.append("Version: ").append(version != null ? version : UNKNOWN_VERSION);
          if (scalaVersion != null) {
            info.append(", Scala: ").append(scalaVersion);
          }
          if (builtDate != null) {
            info.append(", Built: ").append(builtDate);
          }
          if (builtBy != null) {
            info.append(", By: ").append(builtBy);
          }

          return info.toString();
        }
      }
    } catch (Exception e) {
      // Ignore and fall through
    }

    return "Version: " + UNKNOWN_VERSION;
  }

  /** Print version information to console */
  public static void printVersion() {
    System.out.println("Acryl Spark Lineage - " + getDetailedVersion());
  }

  /** Main method for testing or standalone version checking */
  public static void main(String[] args) {
    if (args.length > 0 && "--version".equals(args[0])) {
      printVersion();
      return;
    }

    System.out.println("Simple version: " + getVersion());
    System.out.println("Detailed version: " + getDetailedVersion());
  }
}
