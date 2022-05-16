package datahub.protobuf.visitors.dataset;

import com.linkedin.common.urn.Urn;
import com.linkedin.util.Pair;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;

import java.util.stream.Stream;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;

public class DomainVisitor implements ProtobufModelVisitor<Urn> {

    @Override
    public Stream<Urn> visitGraph(VisitContext context) {
        return ProtobufExtensionUtil.filterByDataHubType(getMessageOptions(context.root().messageProto()),
                        context.getGraph().getRegistry(), ProtobufExtensionUtil.DataHubMetadataType.DOMAIN)
                .stream().map(Pair::getValue).map(o ->
                    Urn.createFromTuple("domain", ((String) o).toLowerCase())
                );
    }
}
