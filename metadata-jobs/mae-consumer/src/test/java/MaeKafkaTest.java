import com.google.common.reflect.ClassPath;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MaeKafkaTest {

    final private AvroData avroData = new AvroData(1);

    @Test
    public void testToConnectRecordWithIllegalNullValue() throws IOException {
        List<String> list = ClassPath.from(ClassLoader.getSystemClassLoader())
                .getTopLevelClasses("com.linkedin.mxe")
                .stream()
                .filter(path -> path.getName().contains("MetadataAuditEvent"))
                .map(x -> x.url().getPath()).collect(Collectors.toList());
        Schema schema = new Schema.Parser().parse(new File("/Users/pedro/dev/oss/datahub/metadata-models/src/mainGeneratedAvroSchema/avro/com/linkedin/mxe/MetadataAuditEvent.avsc"));
        String genericRecordStr = "{\n" +
                "  \"auditHeader\" : null,\n" +
                "  \"oldSnapshot\" : null,\n" +
                "  \"oldSystemMetadata\" : null,\n" +
                "  \"newSnapshot\" : {\n" +
                "    \"com.linkedin.metadata.snapshot.CorpUserSnapshot\" : {\n" +
                "      \"urn\" : \"urn:li:corpuser:aditya@acryl.io\",\n" +
                "      \"aspects\" : [ {\n" +
                "        \"com.linkedin.metadata.key.CorpUserKey\" : {\n" +
                "          \"username\" : \"aditya@acryl.io\"\n" +
                "        }\n" +
                "      } ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"newSystemMetadata\" : {\n" +
                "    \"com.linkedin.mxe.SystemMetadata\" : {\n" +
                "      \"lastObserved\" : {\n" +
                "        \"long\" : 1640105686250\n" +
                "      },\n" +
                "      \"runId\" : null,\n" +
                "      \"registryName\" : null,\n" +
                "      \"registryVersion\" : null,\n" +
                "      \"properties\" : null\n" +
                "    }\n" +
                "  },\n" +
                "  \"operation\" : {\n" +
                "    \"com.linkedin.mxe.MetadataAuditOperation\" : \"UPDATE\"\n" +
                "  }\n" +
                "}\n";
        DecoderFactory decoderFactory = new DecoderFactory();
        Decoder decoder = decoderFactory.jsonDecoder(schema, genericRecordStr);
        DatumReader<GenericData.Record> reader =
                new GenericDatumReader<>(schema);
        GenericRecord record = reader.read(null, decoder);


        try {
            avroData.toConnectData(schema, record);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
