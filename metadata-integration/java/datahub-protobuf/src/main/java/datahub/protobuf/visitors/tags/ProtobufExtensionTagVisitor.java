package datahub.protobuf.visitors.tags;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.tag.TagProperties;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.VisitContext;
import datahub.event.MetadataChangeProposalWrapper;

import java.util.stream.Stream;

public class ProtobufExtensionTagVisitor implements ProtobufModelVisitor<MetadataChangeProposalWrapper<? extends RecordTemplate>> {
    private static final String TAG_PROPERTIES_ASPECT = "tagProperties";

    @Override
    public Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> visitGraph(VisitContext context) {
        return ProtobufExtensionUtil.extractTagPropertiesFromOptions(context.root().messageProto().getOptions()
                        .getAllFields(), context.getGraph().getRegistry())
                .map(ProtobufExtensionTagVisitor::wrapTagProperty);
    }

    @Override
    public Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> visitField(ProtobufField field, VisitContext context) {
        return ProtobufExtensionUtil.extractTagPropertiesFromOptions(field.getFieldProto().getOptions().getAllFields(),
                        context.getGraph().getRegistry())
                .map(ProtobufExtensionTagVisitor::wrapTagProperty);
    }

    private static MetadataChangeProposalWrapper<TagProperties> wrapTagProperty(TagProperties tagProperty) {
        return new MetadataChangeProposalWrapper<>(
                "tag",
                new TagUrn(tagProperty.getName()).toString(),
                ChangeType.UPSERT,
                tagProperty,
                TAG_PROPERTIES_ASPECT);
    }
}
