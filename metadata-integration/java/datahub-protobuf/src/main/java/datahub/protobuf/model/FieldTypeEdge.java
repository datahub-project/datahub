package datahub.protobuf.model;

import lombok.Builder;
import lombok.Getter;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;


@Builder
@Getter
public class FieldTypeEdge extends DefaultEdge {
    @Builder.Default
    private final String type = "";
    @Builder.Default
    private final boolean isMessageType = false;
    private final transient ProtobufElement edgeSource;
    private final transient ProtobufElement edgeTarget;

    public FieldTypeEdge inGraph(DefaultDirectedGraph<ProtobufElement, FieldTypeEdge> g) {
        g.addEdge(edgeSource, edgeTarget, this);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldTypeEdge that = (FieldTypeEdge) o;

        if (isMessageType() != that.isMessageType()) {
            return false;
        }
        if (!getType().equals(that.getType())) {
            return false;
        }
        if (!getEdgeSource().equals(that.getEdgeSource())) {
            return false;
        }
        return getEdgeTarget().equals(that.getEdgeTarget());
    }

    @Override
    public int hashCode() {
        int result = getType().hashCode();
        result = 31 * result + (isMessageType() ? 1 : 0);
        result = 31 * result + getEdgeSource().hashCode();
        result = 31 * result + getEdgeTarget().hashCode();
        return result;
    }
}
