package com.linkedin.metadata.examples.cli;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.Topics;
import com.linkedin.restli.common.ContentType;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import picocli.CommandLine;


@Slf4j
@Component
public class MceCli implements CommandLineRunner {
  private enum Mode {
    PRODUCE, CONSUME
  }

  private static final class Args {
    @CommandLine.Option(names = {"-m", "--mode"}, defaultValue = "CONSUME")
    Mode mode;

    @CommandLine.Parameters(
        paramLabel = "EVENT_FILE",
        description = "MCE file; required if running 'producer' mode. See MetadataChangeEvents.pdl for schema.",
        arity = "0..1"
    )
    File eventFile;
  }

  @Inject
  @Named("kafkaEventProducer")
  private Producer<String, GenericRecord> _producer;

  @Inject
  @Named("kafkaEventConsumer")
  private Consumer<String, GenericRecord> _consumer;

  private void consume() {
    log.info("Consuming records.");

    _consumer.subscribe(ImmutableList.of(Topics.METADATA_CHANGE_EVENT));

    while (true) {
      final ConsumerRecords<String, GenericRecord> records = _consumer.poll(Duration.ofSeconds(1));

      for (ConsumerRecord<String, GenericRecord> record : records) {
        log.info(record.value().toString());
      }
    }
  }

  @VisibleForTesting
  static MetadataChangeEvents readEventsFile(@Nonnull File eventsFile) throws IOException {
    final DataMap dataMap = ContentType.JSON.getCodec().readMap(new FileInputStream(eventsFile));

    final ValidationOptions options = new ValidationOptions();
    options.setRequiredMode(RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT);
    options.setUnrecognizedFieldMode(UnrecognizedFieldMode.DISALLOW);

    final ValidationResult result =
        ValidateDataAgainstSchema.validate(dataMap, DataTemplateUtil.getSchema(MetadataChangeEvents.class), options);

    if (!result.isValid()) {
      throw new IllegalArgumentException(
          String.format("Error parsing metadata events file %s: \n%s", eventsFile.toString(),
              Joiner.on('\n').join(result.getMessages())));
    }

    return DataTemplateUtil.wrap(dataMap, MetadataChangeEvents.class);
  }

  private void produce(@Nonnull File eventsFile) throws IOException, ExecutionException, InterruptedException {
    final MetadataChangeEvents events = readEventsFile(eventsFile);
    int record = 1;
    for (MetadataChangeEvent mce : events.getEvents()) {
      log.info("Producing record {} of {}", record++, events.getEvents().size());
      _producer.send(new ProducerRecord(Topics.METADATA_CHANGE_EVENT, EventUtils.pegasusToAvroMCE(mce))).get();
      log.info("Produced record.");
    }
  }

  @Override
  public void run(String... cmdLineArgs) throws Exception {
    final Args args = new Args();
    new CommandLine(args).setCaseInsensitiveEnumValuesAllowed(true).parseArgs(cmdLineArgs);

    switch (args.mode) {
      case CONSUME:
        consume();
        break;
      case PRODUCE:
        if (args.eventFile == null) {
          throw new IllegalArgumentException("Event file is required when producing.");
        }
        produce(args.eventFile);
        break;
      default:
        break;
    }
  }
}
