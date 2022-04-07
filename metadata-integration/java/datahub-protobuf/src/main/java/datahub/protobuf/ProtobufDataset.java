package datahub.protobuf;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.schema.KafkaSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import datahub.protobuf.model.ProtobufGraph;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import datahub.protobuf.visitors.dataset.DatasetVisitor;
import datahub.protobuf.visitors.dataset.DomainVisitor;
import datahub.protobuf.visitors.dataset.InstitutionalMemoryVisitor;
import datahub.protobuf.visitors.dataset.KafkaTopicPropertyVisitor;
import datahub.protobuf.visitors.dataset.OwnershipVisitor;
import datahub.protobuf.visitors.dataset.ProtobufExtensionPropertyVisitor;
import datahub.protobuf.visitors.dataset.ProtobufExtensionTagAssocVisitor;
import datahub.protobuf.visitors.dataset.ProtobufExtensionTermAssocVisitor;
import datahub.protobuf.visitors.field.SchemaFieldVisitor;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.protobuf.visitors.field.ProtobufExtensionFieldVisitor;
import datahub.protobuf.visitors.tags.ProtobufExtensionTagVisitor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ProtobufDataset {

    public static ProtobufDataset.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private DataPlatformUrn dataPlatformUrn;
        private DatasetUrn datasetUrn;
        private FabricType fabricType;
        private AuditStamp auditStamp;
        private byte[] protocBytes;
        private String messageName;
        private String filename;
        private String schema;
        private String githubOrganization;
        private String slackTeamId;

        public Builder setGithubOrganization(@Nullable String githubOrganization) {
            this.githubOrganization = githubOrganization;
            return this;
        }

        public Builder setSlackTeamId(@Nullable String slackTeamId) {
            this.slackTeamId = slackTeamId;
            return this;
        }

        public Builder setProtocIn(InputStream protocIn) throws IOException {
            return setProtocBytes(protocIn.readAllBytes());
        }

        public Builder setDataPlatformUrn(@Nullable DataPlatformUrn dataPlatformUrn) {
            this.dataPlatformUrn = dataPlatformUrn;
            return this;
        }

        public Builder setDatasetUrn(@Nullable DatasetUrn datasetUrn) {
            this.datasetUrn = datasetUrn;
            return this;
        }

        public Builder setProtocBytes(byte[] protocBytes) {
            this.protocBytes = protocBytes;
            return this;
        }

        public Builder setFabricType(FabricType fabricType) {
            this.fabricType = fabricType;
            return this;
        }

        public Builder setAuditStamp(AuditStamp auditStamp) {
            this.auditStamp = auditStamp;
            return this;
        }

        public Builder setMessageName(@Nullable String messageName) {
            this.messageName = messageName;
            return this;
        }
        public Builder setFilename(@Nullable String filename) {
            this.filename = filename;
            return this;
        }

        public Builder setSchema(@Nullable String schema) {
            this.schema = schema;
            return this;
        }

        public ProtobufDataset build() throws IOException {
            FileDescriptorSet fileSet = FileDescriptorSet.parseFrom(protocBytes);

            return new ProtobufDataset(
                    this,
                    Optional.ofNullable(dataPlatformUrn).orElse(new DataPlatformUrn("kafka")),
                    datasetUrn,
                    new ProtobufGraph(fileSet, messageName, filename), schema, auditStamp, fabricType)
                    .setMetadataChangeProposalVisitors(
                            List.of(
                                    new ProtobufExtensionTagVisitor()
                            )
                    )
                    .setFieldVisitor(new ProtobufExtensionFieldVisitor())
                    .setDatasetVisitor(DatasetVisitor.builder()
                            .protocBase64(Base64.getEncoder().encodeToString(protocBytes))
                            .datasetPropertyVisitors(
                                    List.of(
                                            new KafkaTopicPropertyVisitor(),
                                            new ProtobufExtensionPropertyVisitor()
                                    )
                            )
                            .institutionalMemoryMetadataVisitors(
                                    List.of(
                                            new InstitutionalMemoryVisitor(slackTeamId, githubOrganization)
                                    )
                            )
                            .tagAssociationVisitors(
                                    List.of(
                                            new ProtobufExtensionTagAssocVisitor()
                                    )
                            )
                            .termAssociationVisitors(
                                    List.of(
                                            new ProtobufExtensionTermAssocVisitor()
                                    )
                            )
                            .ownershipVisitors(
                                    List.of(
                                            new OwnershipVisitor()
                                    )
                            )
                            .domainVisitors(
                                    List.of(
                                            new DomainVisitor()
                                    )
                            )
                            .build()
                    );
        }
    }

    private final DatasetUrn datasetUrn;
    private final Optional<String> schemaSource;
    private final ProtobufGraph graph;
    private final AuditStamp auditStamp;
    private final VisitContext.VisitContextBuilder contextBuilder;
    private final ProtobufDataset.Builder builder;

    private DatasetVisitor datasetVisitor;
    private ProtobufModelVisitor<Pair<SchemaField, Double>> fieldVisitor;
    private List<ProtobufModelVisitor<MetadataChangeProposalWrapper<? extends RecordTemplate>>> mcpwVisitors;

    public ProtobufDataset(DataPlatformUrn dataPlatformUrn, DatasetUrn datasetUrn, ProtobufGraph graph, String schema,
                           AuditStamp auditStamp, FabricType fabricType) {
        this(null, dataPlatformUrn, datasetUrn, graph, schema, auditStamp, fabricType);
    }

    public ProtobufDataset(ProtobufDataset.Builder builder, DataPlatformUrn dataPlatformUrn, DatasetUrn datasetUrn, ProtobufGraph graph,
                           String schema, AuditStamp auditStamp, FabricType fabricType) {
        this.builder = builder;
        this.schemaSource = Optional.ofNullable(schema);
        this.auditStamp = auditStamp;
        this.graph = graph;

        // Default - non-protobuf extension
        fieldVisitor = new SchemaFieldVisitor();
        mcpwVisitors = List.of();

        this.datasetUrn = datasetUrn != null ? datasetUrn : new DatasetUrn(dataPlatformUrn, this.graph.getFullName(), fabricType);
        this.contextBuilder = VisitContext.builder().datasetUrn(this.datasetUrn).auditStamp(this.auditStamp);
    }

    public ProtobufDataset setMetadataChangeProposalVisitors(List<ProtobufModelVisitor<MetadataChangeProposalWrapper<? extends RecordTemplate>>> visitors) {
        this.mcpwVisitors = visitors;
        return this;
    }

    public ProtobufDataset setDatasetVisitor(DatasetVisitor datasetVisitor) {
        this.datasetVisitor = datasetVisitor;
        return this;
    }

    public ProtobufDataset setFieldVisitor(ProtobufModelVisitor<Pair<SchemaField, Double>> visitor) {
        this.fieldVisitor = visitor;
        return this;
    }

    public ProtobufDataset.Builder toBuilder() {
        return builder;
    }

    public ProtobufGraph getGraph() {
        return graph;
    }

    public AuditStamp getAuditStamp() {
        return auditStamp;
    }

    public DatasetUrn getDatasetUrn() {
        return datasetUrn;
    }

    public Stream<Collection<MetadataChangeProposalWrapper<? extends RecordTemplate>>> getAllMetadataChangeProposals() {
        return Stream.of(getVisitorMCPs(), getDatasetMCPs());
    }

    public List<MetadataChangeProposalWrapper<? extends RecordTemplate>> getVisitorMCPs() {
        return graph.accept(contextBuilder, mcpwVisitors).collect(Collectors.toList());
    }

    public List<MetadataChangeProposalWrapper<? extends RecordTemplate>> getDatasetMCPs() {
        return Stream.concat(
                this.graph.accept(contextBuilder, List.of(datasetVisitor)),
                Stream.of(
                        new MetadataChangeProposalWrapper<>(DatasetUrn.ENTITY_TYPE, datasetUrn.toString(), ChangeType.UPSERT,
                                getSchemaMetadata(), "schemaMetadata"),
                        new MetadataChangeProposalWrapper<>(DatasetUrn.ENTITY_TYPE, datasetUrn.toString(), ChangeType.UPSERT,
                                new Status().setRemoved(false), "status")
                )
        ).collect(Collectors.toList());
    }

    public SchemaMetadata getSchemaMetadata() {
        SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
        schemaSource.ifPresent(schemaStr -> platformSchema.setKafkaSchema(new KafkaSchema().setDocumentSchema(schemaStr)));

        List<SchemaField> schemaFields = graph.accept(contextBuilder, List.of(fieldVisitor))
                .sorted(COMPARE_BY_ROOT_MESSAGE_FIELD_WEIGHT.thenComparing(COMPARE_BY_FIELD_PATH))
                .map(Pair::getFirst)
                .collect(Collectors.toList());

        return new SchemaMetadata()
                .setSchemaName(graph.getFullName())
                .setPlatform(datasetUrn.getPlatformEntity())
                .setCreated(auditStamp)
                .setLastModified(auditStamp)
                .setVersion(graph.getMajorVersion())
                .setHash(graph.getHash())
                .setPlatformSchema(platformSchema)
                .setFields(new SchemaFieldArray(schemaFields));
    }

    public static final Comparator<Pair<SchemaField, Double>> COMPARE_BY_ROOT_MESSAGE_FIELD_WEIGHT = Comparator.comparing(Pair::getSecond);
    public static final Comparator<Pair<SchemaField, Double>> COMPARE_BY_FIELD_PATH = Comparator
            .comparing(p -> p.getFirst().getFieldPath());
}
