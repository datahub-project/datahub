/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client.kafka;

import com.linkedin.dataset.DatasetProperties;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.File;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroSerializerTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private MetadataChangeProposalWrapper getMetadataChangeProposalWrapper(
      String description, String entityUrn) {
    return MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(entityUrn)
        .upsert()
        .aspect(new DatasetProperties().setDescription(description))
        .build();
  }

  @Test
  public void avroFileWrite() throws Exception {

    AvroSerializer avroSerializer = new AvroSerializer();
    File file = tempFolder.newFile("data.avro");
    DatumWriter<GenericRecord> writer =
        new GenericDatumWriter<GenericRecord>(avroSerializer.getRecordSchema());
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(avroSerializer.getRecordSchema(), file);
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)";
    for (int i = 0; i < 10; ++i) {
      MetadataChangeProposalWrapper metadataChangeProposalWrapper =
          getMetadataChangeProposalWrapper("Test description - " + i, entityUrn);
      GenericRecord record = avroSerializer.serialize(metadataChangeProposalWrapper);
      dataFileWriter.append(record);
    }
    dataFileWriter.close();

    File readerFile = file;
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSerializer.getRecordSchema());
    DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<GenericRecord>(readerFile, reader);
    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();
      System.out.println(record.get("entityUrn"));
      System.out.println(((GenericRecord) record.get("aspect")).get("value"));
    }
  }
}
