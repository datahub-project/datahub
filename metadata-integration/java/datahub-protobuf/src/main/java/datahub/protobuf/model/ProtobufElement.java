package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.List;
import java.util.stream.Stream;

public interface ProtobufElement {
  String name();

  String fullName();

  String nativeType();

  String comment();

  String fieldPathType();

  FileDescriptorProto fileProto();

  DescriptorProto messageProto();

  default Stream<SourceCodeInfo.Location> messageLocations() {
    List<SourceCodeInfo.Location> fileLocations = fileProto().getSourceCodeInfo().getLocationList();
    return fileLocations.stream()
        .filter(
            loc ->
                loc.getPathCount() > 1
                    && loc.getPath(0) == FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER
                    && messageProto() == fileProto().getMessageType(loc.getPath(1)));
  }

  <T> Stream<T> accept(ProtobufModelVisitor<T> v, VisitContext context);
}
