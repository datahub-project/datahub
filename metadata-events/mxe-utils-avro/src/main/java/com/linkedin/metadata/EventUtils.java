package com.linkedin.metadata;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.linkedin.data.avro.DataTranslator;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.FailedMetadataChangeEvent;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;

public class EventUtils {

  private static final RecordDataSchema MCE_PEGASUS_SCHEMA = new MetadataChangeEvent().schema();

  private static final RecordDataSchema MAE_PEGASUS_SCHEMA = new MetadataAuditEvent().schema();

  private static final RecordDataSchema MCP_PEGASUS_SCHEMA = new MetadataChangeProposal().schema();

  private static final RecordDataSchema MCL_PEGASUS_SCHEMA = new MetadataChangeLog().schema();

  private static final RecordDataSchema PE_PEGASUS_SCHEMA = new PlatformEvent().schema();

  private static final RecordDataSchema DUHE_PEGASUS_SCHEMA =
      new DataHubUpgradeHistoryEvent().schema();

  private static final Schema ORIGINAL_MCE_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/MetadataChangeEvent.avsc");

  private static final Schema ORIGINAL_MAE_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/MetadataAuditEvent.avsc");

  private static final Schema ORIGINAL_FAILED_MCE_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/FailedMetadataChangeEvent.avsc");

  private static final Schema ORIGINAL_MCP_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/MetadataChangeProposal.avsc");

  private static final Schema ORIGINAL_MCL_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/MetadataChangeLog.avsc");

  private static final Schema ORIGINAL_FMCL_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/FailedMetadataChangeProposal.avsc");

  private static final Schema ORIGINAL_PE_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/PlatformEvent.avsc");

  public static final Schema ORIGINAL_DUHE_AVRO_SCHEMA =
      getAvroSchemaFromResource("avro/com/linkedin/mxe/DataHubUpgradeHistoryEvent.avsc");

  private static final Schema RENAMED_MCE_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.MetadataChangeEvent.SCHEMA$;

  private static final Schema RENAMED_MAE_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.MetadataAuditEvent.SCHEMA$;

  private static final Schema RENAMED_FAILED_MCE_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent.SCHEMA$;

  private static final Schema RENAMED_PE_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.PlatformEvent.SCHEMA$;

  private static final Schema RENAMED_MCP_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.MetadataChangeProposal.SCHEMA$;

  private static final Schema RENAMED_MCL_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.MetadataChangeLog.SCHEMA$;

  private static final Schema RENAMED_FMCP_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.FailedMetadataChangeProposal.SCHEMA$;

  private static final Schema RENAMED_DUHE_AVRO_SCHEMA =
      com.linkedin.pegasus2avro.mxe.DataHubUpgradeHistoryEvent.SCHEMA$;

  private EventUtils() {
    // Util class
  }

  @Nonnull
  private static Schema getAvroSchemaFromResource(@Nonnull String resourcePath) {
    URL url = EventUtils.class.getClassLoader().getResource(resourcePath);
    try {
      return Schema.parse(Resources.toString(url, Charsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts a {@link GenericRecord} MAE into the equivalent Pegasus model.
   *
   * @param record the {@link GenericRecord} that contains the MAE in com.linkedin.pegasus2avro
   *     namespace
   * @return the Pegasus {@link MetadataAuditEvent} model
   */
  @Nonnull
  public static MetadataAuditEvent avroToPegasusMAE(@Nonnull GenericRecord record)
      throws IOException {
    return new MetadataAuditEvent(
        DataTranslator.genericRecordToDataMap(
            renameSchemaNamespace(record, RENAMED_MAE_AVRO_SCHEMA, ORIGINAL_MAE_AVRO_SCHEMA),
            MAE_PEGASUS_SCHEMA,
            ORIGINAL_MAE_AVRO_SCHEMA));
  }

  /**
   * Converts a {@link GenericRecord} MCE into the equivalent Pegasus model.
   *
   * @param record the {@link GenericRecord} that contains the MCE in com.linkedin.pegasus2avro
   *     namespace
   * @return the Pegasus {@link MetadataChangeEvent} model
   */
  @Nonnull
  public static MetadataChangeEvent avroToPegasusMCE(@Nonnull GenericRecord record)
      throws IOException {
    return new MetadataChangeEvent(
        DataTranslator.genericRecordToDataMap(
            renameSchemaNamespace(record, RENAMED_MCE_AVRO_SCHEMA, ORIGINAL_MCE_AVRO_SCHEMA),
            MCE_PEGASUS_SCHEMA,
            ORIGINAL_MCE_AVRO_SCHEMA));
  }

  /**
   * Converts a {@link GenericRecord} MCL into the equivalent Pegasus model.
   *
   * @param record the {@link GenericRecord} that contains the MCL in com.linkedin.pegasus2avro
   *     namespace
   * @return the Pegasus {@link MetadataChangeLog} model
   */
  @Nonnull
  public static MetadataChangeLog avroToPegasusMCL(@Nonnull GenericRecord record)
      throws IOException {
    return new MetadataChangeLog(
        DataTranslator.genericRecordToDataMap(
            renameSchemaNamespace(record, RENAMED_MCL_AVRO_SCHEMA, ORIGINAL_MCL_AVRO_SCHEMA),
            MCL_PEGASUS_SCHEMA,
            ORIGINAL_MCL_AVRO_SCHEMA));
  }

  /**
   * Converts a {@link GenericRecord} MCP into the equivalent Pegasus model.
   *
   * @param record the {@link GenericRecord} that contains the MCP in com.linkedin.pegasus2avro
   *     namespace
   * @return the Pegasus {@link MetadataChangeProposal} model
   */
  @Nonnull
  public static MetadataChangeProposal avroToPegasusMCP(@Nonnull GenericRecord record)
      throws IOException {
    return new MetadataChangeProposal(
        DataTranslator.genericRecordToDataMap(
            renameSchemaNamespace(record, RENAMED_MCP_AVRO_SCHEMA, ORIGINAL_MCP_AVRO_SCHEMA),
            MCP_PEGASUS_SCHEMA,
            ORIGINAL_MCP_AVRO_SCHEMA));
  }

  /**
   * Converts a {@link GenericRecord} PE into the equivalent Pegasus model.
   *
   * @param record the {@link GenericRecord} that contains the PE in com.linkedin.pegasus2avro
   *     namespace
   * @return the Pegasus {@link PlatformEvent} model
   */
  @Nonnull
  public static PlatformEvent avroToPegasusPE(@Nonnull GenericRecord record) throws IOException {
    return new PlatformEvent(
        DataTranslator.genericRecordToDataMap(
            renameSchemaNamespace(record, RENAMED_PE_AVRO_SCHEMA, ORIGINAL_PE_AVRO_SCHEMA),
            PE_PEGASUS_SCHEMA,
            ORIGINAL_PE_AVRO_SCHEMA));
  }

  /**
   * Converts a {@link GenericRecord} PE into the equivalent Pegasus model.
   *
   * @param record the {@link GenericRecord} that contains the PE in com.linkedin.pegasus2avro
   *     namespace
   * @return the Pegasus {@link PlatformEvent} model
   */
  @Nonnull
  public static DataHubUpgradeHistoryEvent avroToPegasusDUHE(@Nonnull GenericRecord record)
      throws IOException {
    return new DataHubUpgradeHistoryEvent(
        DataTranslator.genericRecordToDataMap(
            renameSchemaNamespace(record, RENAMED_DUHE_AVRO_SCHEMA, ORIGINAL_DUHE_AVRO_SCHEMA),
            DUHE_PEGASUS_SCHEMA,
            ORIGINAL_DUHE_AVRO_SCHEMA));
  }

  /**
   * Converts a Pegasus MAE into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param event the Pegasus {@link MetadataAuditEvent} model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroMAE(@Nonnull MetadataAuditEvent event)
      throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            event.data(), event.schema(), ORIGINAL_MAE_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_MAE_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus MAE into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param event the Pegasus {@link MetadataChangeLog} model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroMCL(@Nonnull MetadataChangeLog event)
      throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            event.data(), event.schema(), ORIGINAL_MCL_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_MCL_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus MAE into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param event the Pegasus {@link MetadataChangeProposal} model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroMCP(@Nonnull MetadataChangeProposal event)
      throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            event.data(), event.schema(), ORIGINAL_MCP_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_MCP_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus MCE into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param event the Pegasus {@link MetadataChangeEvent} model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namesapce
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroMCE(@Nonnull MetadataChangeEvent event)
      throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            event.data(), event.schema(), ORIGINAL_MCE_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_MCE_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus aspect specific MXE into the equivalent Avro model as a {@link
   * GenericRecord}.
   *
   * @param event the Pegasus aspect specific MXE model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static <MXE extends GenericRecord, T extends SpecificRecord>
      MXE pegasusToAvroAspectSpecificMXE(@Nonnull Class<T> clazz, @Nonnull RecordTemplate event)
          throws NoSuchFieldException, IOException, IllegalAccessException {
    final Schema newSchema = (Schema) clazz.getField("SCHEMA$").get(null);
    final Schema originalSchema = getAvroSchemaFromResource(getAvroResourcePath(clazz));
    final GenericRecord original =
        DataTranslator.dataMapToGenericRecord(event.data(), event.schema(), originalSchema);
    return (MXE) renameSchemaNamespace(original, originalSchema, newSchema);
  }

  /**
   * Converts a Pegasus Failed MCE into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param failedMetadataChangeEvent the Pegasus {@link FailedMetadataChangeEvent} model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroFailedMCE(
      @Nonnull FailedMetadataChangeEvent failedMetadataChangeEvent) throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            failedMetadataChangeEvent.data(),
            failedMetadataChangeEvent.schema(),
            ORIGINAL_FAILED_MCE_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_FAILED_MCE_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus Failed MCE into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param failedMetadataChangeProposal the Pegasus {@link FailedMetadataChangeProposal} model
   * @return the Avro model with com.linkedin.pegasus2avro.mxe namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroFailedMCP(
      @Nonnull FailedMetadataChangeProposal failedMetadataChangeProposal) throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            failedMetadataChangeProposal.data(),
            failedMetadataChangeProposal.schema(),
            ORIGINAL_FMCL_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_FMCP_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus Platform Event into the equivalent Avro model as a {@link GenericRecord}.
   *
   * @param event the Pegasus {@link PlatformEvent} model
   * @return the Avro model with com.linkedin.pegasus2avro.event namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroPE(@Nonnull PlatformEvent event) throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            event.data(), event.schema(), ORIGINAL_PE_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_PE_AVRO_SCHEMA);
  }

  /**
   * Converts a Pegasus DataHub Upgrade History Event into the equivalent Avro model as a {@link
   * GenericRecord}.
   *
   * @param event the Pegasus {@link com.linkedin.mxe.DataHubUpgradeHistoryEvent} model
   * @return the Avro model with com.linkedin.pegasus2avro.event namespace
   * @throws IOException if the conversion fails
   */
  @Nonnull
  public static GenericRecord pegasusToAvroDUHE(@Nonnull DataHubUpgradeHistoryEvent event)
      throws IOException {
    GenericRecord original =
        DataTranslator.dataMapToGenericRecord(
            event.data(), event.schema(), ORIGINAL_DUHE_AVRO_SCHEMA);
    return renameSchemaNamespace(original, RENAMED_DUHE_AVRO_SCHEMA);
  }

  /**
   * Converts original MXE into a renamed namespace Does a double convert that should not be
   * necessary since we're already converting prior to calling this method in most spots
   */
  @Nonnull
  @Deprecated
  private static GenericRecord renameSchemaNamespace(
      @Nonnull GenericRecord original, @Nonnull Schema originalSchema, @Nonnull Schema newSchema)
      throws IOException {

    // Step 1: Updates to the latest original schema
    final GenericRecord record = changeSchema(original, original.getSchema(), originalSchema);

    // Step 2: Updates to the new renamed schema
    return changeSchema(record, newSchema, newSchema);
  }

  /** Converts original MXE into a renamed namespace */
  @Nonnull
  private static GenericRecord renameSchemaNamespace(
      @Nonnull GenericRecord original, @Nonnull Schema newSchema) throws IOException {

    return changeSchema(original, newSchema, newSchema);
  }

  /**
   * Changes the schema of a {@link GenericRecord} to a compatible schema
   *
   * <p>Achieved by serializing the record using its embedded schema and deserializing it using the
   * new compatible schema.
   *
   * @param record the record to update schema for
   * @param writerSchema the writer schema to use when deserializing
   * @param readerSchema the reader schema to use when deserializing
   * @return a {@link GenericRecord} using the new {@code readerSchema}
   * @throws IOException
   */
  @Nonnull
  private static GenericRecord changeSchema(
      @Nonnull GenericRecord record, @Nonnull Schema writerSchema, @Nonnull Schema readerSchema)
      throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
      writer.write(record, encoder);
      encoder.flush();
      os.close();

      try (ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray())) {
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        // Must specify both writer & reader schemas for a backward compatible read
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
        return reader.read(null, decoder);
      }
    }
  }

  /**
   * Get Pegasus class from Avro class.
   *
   * @param clazz the aspect specific MXE avro class
   * @return the Pegasus aspect specific MXE class
   * @throws Exception
   */
  public static Class<?> getPegasusClass(@Nonnull Class<?> clazz) throws ClassNotFoundException {
    return Class.forName(clazz.getCanonicalName().replace(".pegasus2avro", ""));
  }

  private static String getAvroResourcePath(@Nonnull Class<?> clazz) {
    return String.format(
        "avro/%s.avsc", clazz.getCanonicalName().replace(".pegasus2avro", "").replace(".", "/"));
  }
}
