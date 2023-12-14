package datahub.protobuf.visitors.field;

import static datahub.protobuf.TestFixtures.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import com.linkedin.util.Pair;
import datahub.protobuf.ProtobufDataset;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class ProtobufExtensionFieldVisitorTest {

  @Test
  public void extendedMessageTest() throws IOException, URISyntaxException {
    ProtobufExtensionFieldVisitor test = new ProtobufExtensionFieldVisitor();
    List<SchemaField> actual =
        getTestProtobufGraph("extended_protobuf", "messageA")
            .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(test))
            .sorted(
                ProtobufDataset.COMPARE_BY_ROOT_MESSAGE_FIELD_WEIGHT.thenComparing(
                    ProtobufDataset.COMPARE_BY_FIELD_PATH))
            .map(Pair::getFirst)
            .collect(Collectors.toList());

    List<SchemaField> expected =
        Stream.of(
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=string].name")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    1),
                Pair.of(
                    new SchemaField()
                        .setFieldPath("[version=2.0].[type=extended_protobuf_Person].[type=int].id")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("int32")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    2),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=string].email")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    3),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=extended_protobuf_Department].dept")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("extended_protobuf.Department")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new RecordType())))
                        .setGlobalTags(
                            new GlobalTags()
                                .setTags(
                                    new TagAssociationArray(
                                        new TagAssociation()
                                            .setTag(new TagUrn("MetaEnumExample.ENTITY")))))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(
                                    new GlossaryTermAssociationArray(
                                        new GlossaryTermAssociation()
                                            .setUrn(
                                                new GlossaryTermUrn("Classification.Sensitive"))))
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    4),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=extended_protobuf_Department].dept.[type=int].id")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("int32")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    4),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=extended_protobuf_Department].dept.[type=string].name")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    4))
            .map(Pair::getFirst)
            .collect(Collectors.toList());

    assertEquals(expected, actual);
  }

  @Test
  public void extendedFieldTest() throws IOException {
    ProtobufExtensionFieldVisitor test = new ProtobufExtensionFieldVisitor();
    List<SchemaField> actual =
        getTestProtobufGraph("extended_protobuf", "messageB")
            .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(test))
            .sorted(
                ProtobufDataset.COMPARE_BY_ROOT_MESSAGE_FIELD_WEIGHT.thenComparing(
                    ProtobufDataset.COMPARE_BY_FIELD_PATH))
            .map(Pair::getFirst)
            .collect(Collectors.toList());

    List<SchemaField> expected =
        Stream.of(
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=string].name")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("person name")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(
                                    new GlossaryTermAssociationArray(
                                        new GlossaryTermAssociation()
                                            .setUrn(
                                                new GlossaryTermUrn(
                                                    "Classification.HighlyConfidential"))))
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    1),
                Pair.of(
                    new SchemaField()
                        .setFieldPath("[version=2.0].[type=extended_protobuf_Person].[type=int].id")
                        .setNullable(false)
                        .setIsPartOfKey(true)
                        .setDescription("unique identifier for a given person")
                        .setNativeDataType("int32")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    2),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=string].email")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("official email address")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(
                                    new GlossaryTermAssociationArray(
                                        new GlossaryTermAssociation()
                                            .setUrn(
                                                new GlossaryTermUrn(
                                                    "Classification.HighlyConfidential"))))
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    3),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=extended_protobuf_Department].dept")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("department name of the person")
                        .setNativeDataType("extended_protobuf.Department")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new RecordType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    4),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=extended_protobuf_Department].dept.[type=int].id")
                        .setNullable(false)
                        .setIsPartOfKey(true)
                        .setDescription("")
                        .setNativeDataType("int32")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    4),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=extended_protobuf_Department].dept.[type=string].name")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    4),
                Pair.of(
                    new SchemaField()
                        .setFieldPath(
                            "[version=2.0].[type=extended_protobuf_Person].[type=string].test_coverage")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setNativeDataType("string")
                        .setType(
                            new SchemaFieldDataType()
                                .setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setGlobalTags(
                            new GlobalTags()
                                .setTags(
                                    new TagAssociationArray(
                                        new TagAssociation()
                                            .setTag(new TagUrn("MetaEnumExample.EVENT")),
                                        new TagAssociation().setTag(new TagUrn("d")),
                                        new TagAssociation().setTag(new TagUrn("deprecated")),
                                        new TagAssociation().setTag(new TagUrn("e")),
                                        new TagAssociation().setTag(new TagUrn("f")),
                                        new TagAssociation()
                                            .setTag(new TagUrn("product_type.my type")),
                                        new TagAssociation()
                                            .setTag(new TagUrn("product_type_bool")))))
                        .setGlossaryTerms(
                            new GlossaryTerms()
                                .setTerms(new GlossaryTermAssociationArray())
                                .setAuditStamp(TEST_AUDIT_STAMP)),
                    5))
            .map(Pair::getFirst)
            .collect(Collectors.toList());

    assertEquals(expected, actual);
  }
}
