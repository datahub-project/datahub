package datahub.protobuf.visitors.dataset;

import com.linkedin.common.GlossaryTermAssociation;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.VisitContext;

import java.util.stream.Stream;

public class ProtobufExtensionTermAssocVisitor implements ProtobufModelVisitor<GlossaryTermAssociation> {

    @Override
    public Stream<GlossaryTermAssociation> visitGraph(VisitContext context) {
        return ProtobufExtensionUtil.extractTermAssociationsFromOptions(context.root().messageProto().getOptions().getAllFields(),
                context.getGraph().getRegistry());
    }
}
