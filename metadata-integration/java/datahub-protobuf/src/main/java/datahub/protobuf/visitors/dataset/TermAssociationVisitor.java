package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.linkedin.common.GlossaryTermAssociation;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.stream.Stream;

public class TermAssociationVisitor implements ProtobufModelVisitor<GlossaryTermAssociation> {

  @Override
  public Stream<GlossaryTermAssociation> visitGraph(VisitContext context) {
    return ProtobufExtensionUtil.extractTermAssociationsFromOptions(
        getMessageOptions(context.root().messageProto()), context.getGraph().getRegistry());
  }
}
