package com.linkedin.metadata.generator;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;


/**
 * Utility used in generating resource classes.
 */
public class SchemaGeneratorUtil {

  private SchemaGeneratorUtil() {
  }

  /**
   * De-Capitalize the input name.
   *
   * @param name the string whose first character will be converted to lowercase.
   * @return the converted name
   */
  @Nonnull
  public static String deCapitalize(@Nonnull String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException();
    } else {
      return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
  }

  /**
   * Strip the urn namespace to the entity name.
   *
   * @param urn the namespace of the entityUrn.
   * @return the entity name.
   */
  @Nonnull
  public static String getEntityName(@Nonnull String urn) {
    return stripNamespace(urn).substring(0,
        stripNamespace(urn).length() - 3); // Truncate the urn to entity name such as: FooBarUrn -> FooBar
  }

  /**
   * Strip the namespace to the valueType.
   *
   * @param namespace the namespace of the entity.
   * @return the valueType of the namespace.
   */
  @Nonnull
  public static String stripNamespace(@Nonnull String namespace) {
    final int index = namespace.lastIndexOf('.');
    if (index < namespace.length() - 1) {
      // index == -1 (dot not found) || 0 <= index < length -1 (namespace is not ended with dot)
      return namespace.substring(index + 1);
    }
    throw new IllegalArgumentException();
  }

  /**
   * Write the content to the file.
   *
   * @param file the target file.
   * @param content the content to be written in the file.
   */
  public static void writeToFile(@Nonnull File file, @Nonnull String content) throws IOException {
    Files.asCharSink(file, Charsets.UTF_8).write(content);
  }

  /**
   * Create event schema output folder.
   *
   * @param eventSchemaOutput the output path for the rendered schemas.
   * @return the directory of the output path.
   */
  public static File createOutputFolder(@Nonnull String eventSchemaOutput) throws IOException {
    final File directory = new File(eventSchemaOutput);
    if (!directory.mkdirs() && !directory.exists()) {
      throw new IOException(String.format("%s cannot be created.", directory));
    }
    return directory;
  }
}