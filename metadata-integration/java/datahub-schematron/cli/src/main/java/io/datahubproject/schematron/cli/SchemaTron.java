package io.datahubproject.schematron.cli;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import datahub.client.Emitter;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import io.datahubproject.schematron.converters.avro.AvroSchemaConverter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(
    name = "schema-translator",
    description = "Converts schemas to DataHub format and emits them",
    mixinStandardHelpOptions = true)
public class SchemaTron implements Callable<Integer> {

  @Option(
      names = {"-i", "--input"},
      description = "Input schema file or directory")
  private String input;

  @Option(
      names = {"-s", "--server"},
      description = "DataHub server URL",
      required = false,
      defaultValue = "http://localhost:8080")
  private String server;

  @Option(
      names = {"-t", "--token"},
      description = "DataHub access token",
      required = false,
      defaultValue = "")
  private String token;

  @Option(
      names = {"-p", "--platform"},
      description = "Data platform name",
      defaultValue = "avro")
  private String platform;

  @Option(
      names = {"--sink"},
      description = "DataHub sink name",
      defaultValue = "rest")
  private String sink;

  @Option(
      names = {"--output-file"},
      description = "Output file for the emitted metadata",
      defaultValue = "metadata.json")
  private String outputFile;

  private final AvroSchemaConverter schemaConverter = AvroSchemaConverter.builder().build();

  @Override
  public Integer call() throws Exception {

    Emitter emitter;
    if (sink.equals("rest")) {
      emitter = RestEmitter.create(b -> b.server(server).token(token));
    } else if (sink.equals("file")) {
      emitter = new FileEmitter(FileEmitterConfig.builder().fileName(outputFile).build());
    } else {
      throw new IllegalArgumentException("Unsupported sink: " + sink);
    }

    try {
      // Process input files
      Stream<Path> inputFiles;
      Path inputPath = Path.of(input);
      if (Files.isDirectory(inputPath)) {
        inputFiles = Files.walk(inputPath).filter(p -> p.toString().endsWith(".avsc"));
      } else {
        inputFiles = Stream.of(inputPath);
      }

      // Process each file
      inputFiles.forEach(
          filePath -> {
            try {
              // Read and parse Avro schema
              String schemaStr = Files.readString(filePath);
              Schema avroSchema = new Schema.Parser().parse(schemaStr);

              // Convert to DataHub schema
              boolean isKeySchema = false;
              boolean isDefaultNullable = false;
              SchemaMetadata schemaMetadata =
                  schemaConverter.toDataHubSchema(
                      avroSchema,
                      isKeySchema,
                      isDefaultNullable,
                      new DataPlatformUrn(platform),
                      null);
              log.info("Generated {} fields", schemaMetadata.getFields().size());
              for (SchemaField field : schemaMetadata.getFields()) {
                log.debug("Field path: {}", field.getFieldPath());
              }

              DatasetUrn datasetUrn =
                  new DatasetUrn(
                      new DataPlatformUrn(platform), avroSchema.getFullName(), FabricType.PROD);

              MetadataChangeProposalWrapper<SchemaMetadata> wrapper =
                  new MetadataChangeProposalWrapper(
                      "dataset",
                      datasetUrn.toString(),
                      ChangeType.UPSERT,
                      schemaMetadata,
                      "schemaMetadata");

              // Emit to DataHub
              emitter.emit(wrapper, null).get();
              log.info("Emitted schema for {}", datasetUrn);
            } catch (Exception e) {
              System.err.println("Error processing file: " + filePath);
              e.printStackTrace();
            }
          });

      return 0;
    } finally {
      emitter.close();
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new SchemaTron()).execute(args);
    System.exit(exitCode);
  }
}
