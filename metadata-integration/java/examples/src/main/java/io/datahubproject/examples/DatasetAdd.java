package io.datahubproject.examples;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.schema.DateType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DatasetAdd {

  private DatasetAdd() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    DatasetUrn datasetUrn = UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD");
    CorpuserUrn userUrn = new CorpuserUrn("ingestion");
    AuditStamp lastModified = new AuditStamp().setTime(1640692800000L).setActor(userUrn);

    SchemaMetadata schemaMetadata =
        new SchemaMetadata()
            .setSchemaName("customer")
            .setPlatform(new DataPlatformUrn("hive"))
            .setVersion(0L)
            .setHash("")
            .setPlatformSchema(
                SchemaMetadata.PlatformSchema.create(
                    new OtherSchema().setRawSchema("__insert raw schema here__")))
            .setLastModified(lastModified);

    SchemaFieldArray fields = new SchemaFieldArray();

    SchemaField field1 =
        new SchemaField()
            .setFieldPath("address.zipcode")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("VARCHAR(50)")
            .setDescription(
                "This is the zipcode of the address. Specified using extended form and limited to addresses in the United States")
            .setLastModified(lastModified);
    fields.add(field1);

    SchemaField field2 =
        new SchemaField()
            .setFieldPath("address.street")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("VARCHAR(100)")
            .setDescription("Street corresponding to the address")
            .setLastModified(lastModified);
    fields.add(field2);

    SchemaField field3 =
        new SchemaField()
            .setFieldPath("last_sold_date")
            .setType(
                new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType())))
            .setNativeDataType("Date")
            .setDescription("Date of the last sale date for this property")
            .setLastModified(lastModified);
    fields.add(field3);

    schemaMetadata.setFields(fields);

    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(schemaMetadata)
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response = emitter.emit(mcpw, null);
    System.out.println(response.get().getResponseContent());
  }
}
