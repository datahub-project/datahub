package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.linkedin.schema.MapType;
import com.linkedin.schema.RecordType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import java.util.Arrays;
import java.util.stream.Stream;
import com.linkedin.schema.SchemaFieldDataType;

import datahub.protobuf.ProtobufUtils;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;



@Builder
@AllArgsConstructor
public class ProtobufMessage implements ProtobufElement {
    private final DescriptorProto messageProto;
    private final DescriptorProto parentMessageProto;
    private final FileDescriptorProto fileProto;

    @Override
    public String name() {
        return messageProto.getName();
    }

    @Override
    public String fullName() {
        if (parentMessageProto  != null) {
            return String.join(".", fileProto.getPackage(), parentMessageProto.getName(), name());
        }
        return String.join(".", fileProto.getPackage(), name());
    }

    @Override
    public String nativeType() {
        return fullName();
    }

    @Override
    public String fieldPathType() {
        return String.format("[type=%s]", nativeType().replace(".", "_"));
    }

    @Override
    public FileDescriptorProto fileProto() {
        return fileProto;
    }

    @Override
    public DescriptorProto messageProto() {
        return messageProto;
    }

    public SchemaFieldDataType schemaFieldDataType() {
        if (parentMessageProto != null && messageProto.getName().equals("MapFieldEntry")) {
            return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new MapType()));
        }
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType()));
    }

    public int majorVersion() {
        return Integer.parseInt(Arrays.stream(fileProto.getName().split("/"))
                .filter(p -> p.matches("^v[0-9]+$"))
                .findFirst()
                .map(p -> p.replace("v", ""))
                .orElse("1"));
    }

    @Override
    public String comment() {
        return messageLocations()
                .map(ProtobufUtils::collapseLocationComments)
                .findFirst().orElse("");
    }

    @Override
    public <T> Stream<T> accept(ProtobufModelVisitor<T> visitor, VisitContext context) {
        return visitor.visitMessage(this, context);
    }

    @Override
    public String toString() {
        return String.format("ProtobufMessage[%s]", fullName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProtobufMessage that = (ProtobufMessage) o;

        if (!fullName().equals(that.fullName())) {
            return false;
        }
        if (!messageProto.equals(that.messageProto)) {
            return false;
        }
        if (parentMessageProto != null ? !parentMessageProto.equals(that.parentMessageProto) : that.parentMessageProto != null) {
            return false;
        }
        return fileProto.equals(that.fileProto);
    }

    @Override
    public int hashCode() {
        int result = messageProto.hashCode();
        result = 31 * result + (parentMessageProto != null ? parentMessageProto.hashCode() : 0);
        result = 31 * result + fileProto.hashCode();
        result = 31 * result + fullName().hashCode();
        return result;
    }
}
