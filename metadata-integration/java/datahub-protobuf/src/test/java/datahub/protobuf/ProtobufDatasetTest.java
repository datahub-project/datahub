package datahub.protobuf;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.data.template.StringArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.FabricType;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.UnionType;
import com.linkedin.util.Pair;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static datahub.protobuf.TestFixtures.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ProtobufDatasetTest {

    @Test
    public void noSchemaTest() throws IOException {
        ProtobufDataset dataset = ProtobufDataset.builder()
                .setDataPlatformUrn(new DataPlatformUrn("kafka"))
                .setProtocIn(getTestProtoc("protobuf", "messageA"))
                .setAuditStamp(TEST_AUDIT_STAMP)
                .setFabricType(FabricType.DEV)
                .build();

        assertNotNull(dataset);
        assertEquals(2, dataset.getAllMetadataChangeProposals().count());
        assertEquals(8, dataset.getDatasetMCPs().size());
        assertEquals(0, dataset.getVisitorMCPs().size());
    }

    @Test
    public void platformSchemaTest() throws IOException {
        assertEquals(getTestProtoSource("protobuf", "messageA"),
                extractDocumentSchema(getTestProtobufDataset("protobuf", "messageA")));
    }

    @Test
    public void messageA() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageA");

        assertEquals("urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageA,TEST)",
                test.getDatasetUrn().toString());

        SchemaMetadata testMetadata = test.getSchemaMetadata();

        assertEquals(1, testMetadata.getVersion());
        assertEquals(9, testMetadata.getFields().size());


        assertEquals("platform.topic", extractCustomProperty(test.getDatasetMCPs().get(0), "kafka_topic"));

        assertEquals(new InstitutionalMemory().setElements(new InstitutionalMemoryMetadataArray(
                        new InstitutionalMemoryMetadata()
                                .setDescription("Github Team")
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setUrl(new Url("https://github.com/orgs/myOrg/teams/teama")),
                        new InstitutionalMemoryMetadata()
                                .setDescription("Slack Channel")
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setUrl(new Url("https://slack.com/app_redirect?channel=test-slack&team=SLACK123")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA Reference 1")
                                .setUrl(new Url("https://some/link")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA Reference 2")
                                .setUrl(new Url("https://www.google.com/search?q=protobuf+messages")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA Reference 3")
                                .setUrl(new Url("https://github.com/apache/kafka")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA.map_field Reference 1")
                                .setUrl(new Url("https://developers.google.com/protocol-buffers/docs/proto3#maps")))).data(),
                test.getDatasetMCPs().get(1).getAspect().data());

        assertEquals(new Status().setRemoved(false).data(), test.getDatasetMCPs().get(test.getDatasetMCPs().size() - 1).getAspect().data());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageA].[type=bytes].sequence_id")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType())))
                        .setNativeDataType("bytes")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("Leading single line comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(TEST_AUDIT_STAMP)),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageA].[type=bytes].sequence_id")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageA].[type=int].position")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setNativeDataType("uint32")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("Leading multiline comment\nSecond line of leading multiline comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(TEST_AUDIT_STAMP)),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageA].[type=int].position")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageA].[type=int].total")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setNativeDataType("uint32")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("Detached comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(TEST_AUDIT_STAMP)),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageA].[type=int].total")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageA].[type=array].[type=long].repeated_num")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType().setNestedType(new StringArray()))))
                        .setNativeDataType("uint64")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("Test repeated and trailing comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(TEST_AUDIT_STAMP)),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageA].[type=array].[type=long].repeated_num")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageA].[type=array].[type=string].repeated_str")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType().setNestedType(new StringArray()))))
                        .setNativeDataType("string")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setDescription("")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(TEST_AUDIT_STAMP)),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageA].[type=array].[type=string].repeated_str")).findFirst().orElseThrow());

    }

    @Test
    public void messageB() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageB");

        assertEquals("urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageB,TEST)",
                test.getDatasetUrn().toString());

        SchemaMetadata testMetadata = test.getSchemaMetadata();

        assertEquals(1, testMetadata.getVersion());
        assertEquals(24, testMetadata.getFields().size());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageB].[type=long].id")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setNativeDataType("google.protobuf.Int64Value")
                        .setDescription("wrapped int64")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageB].[type=long].id")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageB].[type=boolean].hot")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())))
                        .setNativeDataType("google.protobuf.BoolValue")
                        .setDescription("Indicator")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageB].[type=boolean].hot")).findFirst().orElseThrow());


        assertEquals(new SchemaField()
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setFieldPath("[version=2.0].[type=protobuf_MessageB].[type=string].value")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setNativeDataType("string")
                        .setDescription("message value")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageB].[type=string].value")).findFirst().orElseThrow());
    }

    @Test
    public void messageC() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageC");


        assertEquals("urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageC,TEST)",
                test.getDatasetUrn().toString());

        SchemaMetadata testMetadata = test.getSchemaMetadata();

        assertEquals(1, testMetadata.getVersion());
        assertEquals(4, testMetadata.getFields().size());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType())))
                        .setNativeDataType("oneof")
                        .setDescription("one of field comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field.[type=string].one_of_string")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setNativeDataType("string")
                        .setDescription("one of string comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field.[type=string].one_of_string")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field.[type=int].one_of_int")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setNativeDataType("int32")
                        .setDescription("one of int comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC].[type=union].one_of_field.[type=int].one_of_int")).findFirst().orElseThrow());
    }

    @Test
    @SuppressWarnings("LineLength")
    public void messageC2NestedOneOf() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageC2");


        assertEquals("urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageC1,TEST)",
                test.getDatasetUrn().toString());

        SchemaMetadata testMetadata = test.getSchemaMetadata();

        assertEquals(1, testMetadata.getVersion());
        assertEquals(6, testMetadata.getFields().size());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())))
                        .setNativeDataType("protobuf.MessageC2")
                        .setDescription("")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType().setNestedType(new StringArray()))))
                        .setNativeDataType("protobuf.MessageC3")
                        .setDescription("")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=string].normal")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setNativeDataType("string")
                        .setDescription("")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=string].normal")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=union].one_of_field")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType())))
                        .setNativeDataType("oneof")
                        .setDescription("one of field comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=union].one_of_field")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=union].one_of_field.[type=string].one_of_string")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))
                        .setNativeDataType("string")
                        .setDescription("one of string comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=union].one_of_field.[type=string].one_of_string")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setFieldPath("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=union].one_of_field.[type=int].one_of_int")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setNativeDataType("int32")
                        .setDescription("one of int comment")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageC1].[type=protobuf_MessageC2].messageList.[type=array].[type=protobuf_MessageC3].list.[type=union].one_of_field.[type=int].one_of_int")).findFirst().orElseThrow());
    }

    @Test
    public void customFieldVisitors() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageA");

        test.setFieldVisitor(new ProtobufModelVisitor<Pair<SchemaField, Double>>() {
            @Override
            public Stream<Pair<SchemaField, Double>> visitField(ProtobufField field, VisitContext context) {
                if (field.fullName().equals("protobuf.MessageA.sequence_id")) {
                    return Stream.of(Pair.of(
                            new SchemaField()
                                    .setDescription("my comment")
                                    .setNativeDataType("my type")
                                    .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()))),
                            0d));
                } else {
                    return Stream.of();
                }
            }
        });
        assertEquals(1, test.getSchemaMetadata().getFields().size());
        assertEquals(new SchemaField()
                        .setDescription("my comment")
                        .setNativeDataType("my type")
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()))),
                test.getSchemaMetadata().getFields().get(0));
    }

    @Test
    public void duplicateNested() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageB");

        assertEquals("urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageB,TEST)",
                test.getDatasetUrn().toString());

        SchemaMetadata testMetadata = test.getSchemaMetadata();

        assertEquals(1, testMetadata.getVersion());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageB].[type=protobuf_MessageA].nested")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())))
                        .setNativeDataType("protobuf.MessageA")
                        .setDescription("nested message a")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageB].[type=protobuf_MessageA].nested")).findFirst().orElseThrow());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageB].[type=protobuf_MessageA].secondary_nested")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())))
                        .setNativeDataType("protobuf.MessageA")
                        .setDescription("nested message a second time")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageB].[type=protobuf_MessageA].secondary_nested")).findFirst().orElseThrow());

        Set<String> firstNested = testMetadata.getFields().stream().map(SchemaField::getFieldPath)
                .filter(f -> f.contains(".nested"))
                .collect(Collectors.toSet());
        Set<String> secondNested = testMetadata.getFields().stream().map(SchemaField::getFieldPath)
                .filter(f -> f.contains(".secondary_nested"))
                .collect(Collectors.toSet());

        assertEquals(firstNested.size(), secondNested.size());
        assertEquals(firstNested.stream().map(s -> s.replace(".nested", ".secondary_nested")).collect(Collectors.toSet()), secondNested);
    }

    @Test
    public void googleTimestamp() throws IOException {
        ProtobufDataset test = getTestProtobufDataset("protobuf", "messageB");

        assertEquals("urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageB,TEST)",
                test.getDatasetUrn().toString());

        SchemaMetadata testMetadata = test.getSchemaMetadata();

        assertEquals(1, testMetadata.getVersion());

        assertEquals(new SchemaField()
                        .setFieldPath("[version=2.0].[type=protobuf_MessageB].[type=long].time")
                        .setNullable(true)
                        .setIsPartOfKey(false)
                        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())))
                        .setNativeDataType("google.protobuf.Timestamp")
                        .setDescription("google timestamp")
                        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray()))
                        .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()).setAuditStamp(test.getAuditStamp())),
                testMetadata.getFields().stream().filter(f -> f.getFieldPath()
                        .equals("[version=2.0].[type=protobuf_MessageB].[type=long].time")).findFirst().orElseThrow());
    }
}
