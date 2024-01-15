package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.TagUrn;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.stream.Stream;

public class TagAssociationVisitor implements ProtobufModelVisitor<TagAssociation> {

  @Override
  public Stream<TagAssociation> visitGraph(VisitContext context) {
    return ProtobufExtensionUtil.extractTagPropertiesFromOptions(
            getMessageOptions(context.root().messageProto()), context.getGraph().getRegistry())
        .map(tag -> new TagAssociation().setTag(new TagUrn(tag.getName())));
  }
}
