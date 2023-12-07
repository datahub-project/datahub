package datahub.protobuf.visitors.field;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import com.linkedin.schema.UnionType;
import com.linkedin.util.Pair;
import datahub.protobuf.ProtobufDataset;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class SchemaFieldVisitorTest {

  @Test
  public void visitorTest() throws IOException {
    List<SchemaField> expected =
        Stream.of(
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field")
                        .setNullable(true)
                        .setDescription("one of field comment")
                        .setNativeDataType("oneof")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new UnionType()))),
                    1),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field.[type=string].one_of_string")
                        .setNullable(true)
                        .setDescription("one of string comment")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType()))),
                    1),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field.[type=int].one_of_int")
                        .setNullable(true)
                        .setDescription("one of int comment")
                        .setNativeDataType("int32")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new NumberType()))),
                    2),
                Pair.of(
                    new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC].[type=string].normal")
                        .setNullable(true)
                        .setDescription("")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType()))),
                    4))
            .map(Pair::getFirst)
            .collect(Collectors.toList());

    SchemaFieldVisitor test = new SchemaFieldVisitor();
    assertEquals(
        expected,
        getTestProtobufGraph("protobuf", "messageC")
            .accept(getVisitContextBuilder("protobuf.MessageC"), List.of(test))
            .sorted(
                ProtobufDataset.COMPARE_BY_ROOT_MESSAGE_FIELD_WEIGHT.thenComparing(
                    ProtobufDataset.COMPARE_BY_FIELD_PATH))
            .map(Pair::getFirst)
            .collect(Collectors.toList()));
  }
}
