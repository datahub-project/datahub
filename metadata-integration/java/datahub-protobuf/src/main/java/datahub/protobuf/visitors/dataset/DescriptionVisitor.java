package datahub.protobuf.visitors.dataset;

import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;

import java.util.stream.Stream;

public class DescriptionVisitor implements ProtobufModelVisitor<String> {

    @Override
    public Stream<String> visitGraph(VisitContext context) {
        return Stream.of(context.root().comment());
    }
}
