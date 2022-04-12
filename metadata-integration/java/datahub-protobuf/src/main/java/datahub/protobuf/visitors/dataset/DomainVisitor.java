package datahub.protobuf.visitors.dataset;

import com.linkedin.common.urn.Urn;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;

import java.util.stream.Stream;

public class DomainVisitor implements ProtobufModelVisitor<Urn> {

    @Override
    public Stream<Urn> visitGraph(VisitContext context) {
        return ProtobufExtensionUtil.filterByDataHubType(context.root().messageProto()
                        .getOptions().getAllFields(), context.getGraph().getRegistry(), ProtobufExtensionUtil.DataHubMetadataType.DOMAIN)
                .values().stream().map(o ->
                    Urn.createFromTuple("domain", ((String) o).toLowerCase())
                );
    }
}
