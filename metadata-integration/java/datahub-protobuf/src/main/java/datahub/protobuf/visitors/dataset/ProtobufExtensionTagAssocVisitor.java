package datahub.protobuf.visitors.dataset;

import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.TagUrn;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.VisitContext;

import java.util.stream.Stream;


public class ProtobufExtensionTagAssocVisitor implements ProtobufModelVisitor<TagAssociation> {

    @Override
    public Stream<TagAssociation> visitGraph(VisitContext context) {
        return ProtobufExtensionUtil.extractTagPropertiesFromOptions(context.root().messageProto().getOptions()
                        .getAllFields(), context.getGraph().getRegistry())
                .map(tag -> new TagAssociation().setTag(new TagUrn(tag.getName())));
    }
}
