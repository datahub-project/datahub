package com.linkedin.metadata.generator;

import com.linkedin.data.schema.generator.AbstractGenerator;
import java.io.IOException;
import com.linkedin.pegasus.generator.DataSchemaParser;
import javax.annotation.Nonnull;
import org.rythmengine.Rythm;


public class SchemaGenerator {

  private final DataSchemaParser _dataSchemaParser;

  public SchemaGenerator(@Nonnull String resolverPath) {
    _dataSchemaParser = new DataSchemaParser(resolverPath);
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Usage: cmd <sourcePathInput> <generatedFileOutput>");
      System.exit(-1);
    }

    final String source = args[0];
    final SchemaAnnotationRetriever schemaAnnotationRetriever =
        new SchemaAnnotationRetriever(System.getProperty(AbstractGenerator.GENERATOR_RESOLVER_PATH));
    final String[] sources = {source};
    schemaAnnotationRetriever.generate(sources);

    final String mainOutput = args[1];
    final EventSchemaComposer eventSchemaComposer = new EventSchemaComposer();
    eventSchemaComposer.setupRythmEngine();
    eventSchemaComposer.render(schemaAnnotationRetriever.generate(sources), mainOutput);

    Rythm.shutdown();
  }
}