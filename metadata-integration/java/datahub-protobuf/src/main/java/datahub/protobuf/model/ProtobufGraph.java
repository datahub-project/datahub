package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import datahub.protobuf.ProtobufUtils;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.AllDirectedPaths;
import org.jgrapht.graph.DefaultDirectedGraph;

public class ProtobufGraph extends DefaultDirectedGraph<ProtobufElement, FieldTypeEdge> {
  private final transient ProtobufMessage rootProtobufMessage;
  private final transient AllDirectedPaths<ProtobufElement, FieldTypeEdge> directedPaths;
  private final transient ExtensionRegistry registry;

  public ProtobufGraph(DescriptorProtos.FileDescriptorSet fileSet)
      throws InvalidProtocolBufferException {
    this(fileSet, null, null, true);
  }

  public ProtobufGraph(DescriptorProtos.FileDescriptorSet fileSet, String messageName)
      throws InvalidProtocolBufferException {
    this(fileSet, messageName, null, true);
  }

  public ProtobufGraph(
      DescriptorProtos.FileDescriptorSet fileSet, String messageName, String relativeFilename)
      throws InvalidProtocolBufferException {
    this(fileSet, messageName, relativeFilename, true);
  }

  public ProtobufGraph(
      DescriptorProtos.FileDescriptorSet fileSet,
      String messageName,
      String filename,
      boolean flattenGoogleWrapped)
      throws InvalidProtocolBufferException {
    super(FieldTypeEdge.class);
    this.registry = ProtobufUtils.buildRegistry(fileSet);
    DescriptorProtos.FileDescriptorSet fileSetExtended =
        DescriptorProtos.FileDescriptorSet.parseFrom(fileSet.toByteArray(), this.registry);
    buildProtobufGraph(fileSetExtended);
    if (flattenGoogleWrapped) {
      flattenGoogleWrapped();
    }

    if (messageName != null) {
      this.rootProtobufMessage = findMessage(messageName);
    } else {
      DescriptorProtos.FileDescriptorProto lastFile =
          fileSetExtended.getFileList().stream()
              .filter(f -> filename != null && filename.endsWith(f.getName()))
              .findFirst()
              .orElse(fileSetExtended.getFile(fileSetExtended.getFileCount() - 1));

      if (filename != null) {
        this.rootProtobufMessage =
            autodetectRootMessage(lastFile)
                .orElse(
                    autodetectSingleMessage(lastFile)
                        .orElse(
                            autodetectLocalFileRootMessage(lastFile)
                                .orElseThrow(
                                    () ->
                                        new IllegalArgumentException(
                                            "Cannot autodetect protobuf Message."))));
      } else {
        this.rootProtobufMessage =
            autodetectRootMessage(lastFile)
                .orElseThrow(
                    () -> new IllegalArgumentException("Cannot autodetect root protobuf Message."));
      }
    }

    this.directedPaths = new AllDirectedPaths<>(this);
  }

  public List<GraphPath<ProtobufElement, FieldTypeEdge>> getAllPaths(
      ProtobufElement a, ProtobufElement b) {
    return directedPaths.getAllPaths(a, b, true, null);
  }

  public ExtensionRegistry getRegistry() {
    return registry;
  }

  public String getFullName() {
    return rootProtobufMessage.fullName();
  }

  public int getMajorVersion() {
    return rootProtobufMessage.majorVersion();
  }

  public String getComment() {
    return rootProtobufMessage.comment();
  }

  public ProtobufMessage root() {
    return rootProtobufMessage;
  }

  public <T, V extends ProtobufModelVisitor<T>> Stream<T> accept(
      VisitContext.VisitContextBuilder contextBuilder, Collection<V> visitors) {
    VisitContext context =
        Optional.ofNullable(contextBuilder).orElse(VisitContext.builder()).graph(this).build();
    return accept(context, visitors);
  }

  public <T, V extends ProtobufModelVisitor<T>> Stream<T> accept(
      VisitContext context, Collection<V> visitors) {
    return Stream.concat(
        visitors.stream().flatMap(visitor -> visitor.visitGraph(context)),
        vertexSet().stream()
            .flatMap(
                vertex -> visitors.stream().flatMap(visitor -> vertex.accept(visitor, context))));
  }

  protected Optional<ProtobufMessage> autodetectRootMessage(
      DescriptorProtos.FileDescriptorProto targetFile) throws IllegalArgumentException {
    return vertexSet().stream()
        .filter(
            v -> // incoming edges of fields
            targetFile.equals(v.fileProto())
                    && v instanceof ProtobufMessage
                    && incomingEdgesOf(v).isEmpty()
                    && outgoingEdgesOf(v).stream()
                        .flatMap(e -> incomingEdgesOf(e.getEdgeTarget()).stream())
                        .allMatch(
                            e ->
                                e.getEdgeSource()
                                    .equals(
                                        v))) // all the incoming edges on the child vertices should
        // be self
        .map(v -> (ProtobufMessage) v)
        .findFirst();
  }

  protected Optional<ProtobufMessage> autodetectSingleMessage(
      DescriptorProtos.FileDescriptorProto targetFile) throws IllegalArgumentException {
    return vertexSet().stream()
        .filter(
            v -> // incoming edges of fields
            targetFile.equals(v.fileProto())
                    && v instanceof ProtobufMessage
                    && targetFile.getMessageTypeCount() == 1)
        .map(v -> (ProtobufMessage) v)
        .findFirst();
  }

  protected Optional<ProtobufMessage> autodetectLocalFileRootMessage(
      DescriptorProtos.FileDescriptorProto targetFile) throws IllegalArgumentException {
    return vertexSet().stream()
        .filter(
            v -> // incoming edges of fields
            targetFile.equals(v.fileProto())
                    && v instanceof ProtobufMessage
                    && incomingEdgesOf(v).stream()
                        .noneMatch(e -> e.getEdgeSource().fileProto().equals(targetFile))
                    && outgoingEdgesOf(v)
                        .stream() // all the incoming edges on the child vertices should be self
                        // within target file
                        .flatMap(e -> incomingEdgesOf(e.getEdgeTarget()).stream())
                        .allMatch(
                            e ->
                                !e.getEdgeSource().fileProto().equals(targetFile)
                                    || e.getEdgeSource().equals(v)))
        .map(v -> (ProtobufMessage) v)
        .findFirst();
  }

  public ProtobufMessage findMessage(String messageName) throws IllegalArgumentException {
    return (ProtobufMessage)
        vertexSet().stream()
            .filter(v -> v instanceof ProtobufMessage && messageName.equals(v.fullName()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Cannot find protobuf Message %s", messageName)));
  }

  private void buildProtobufGraph(DescriptorProtos.FileDescriptorSet fileSet) {
    // Attach non-nested fields to messages
    fileSet
        .getFileList()
        .forEach(
            fileProto ->
                fileProto
                    .getMessageTypeList()
                    .forEach(
                        messageProto -> {
                          ProtobufMessage messageVertex =
                              ProtobufMessage.builder()
                                  .fileProto(fileProto)
                                  .messageProto(messageProto)
                                  .build();
                          addVertex(messageVertex);

                          // Handle nested fields
                          addNestedMessage(fileProto, messageProto);

                          // Add enum types
                          addEnum(fileProto, messageProto);

                          // handle normal fields and oneofs
                          messageProto
                              .getFieldList()
                              .forEach(
                                  fieldProto -> {
                                    ProtobufField fieldVertex =
                                        ProtobufField.builder()
                                            .protobufMessage(messageVertex)
                                            .fieldProto(fieldProto)
                                            .isNestedType(false)
                                            .build();

                                    // Add field vertex
                                    addVertex(fieldVertex);

                                    if (fieldVertex.oneOfProto() != null) {
                                      // Handle oneOf
                                      addOneOf(messageVertex, fieldVertex);
                                    } else {
                                      // Add schema to field edge
                                      linkMessageToField(messageVertex, fieldVertex);
                                    }
                                  });
                        }));

    // attach field paths to root message
    Map<String, List<ProtobufField>> fieldMap =
        vertexSet().stream()
            .filter(
                v ->
                    v instanceof ProtobufField
                        && incomingEdgesOf(v).stream()
                            .noneMatch(e -> e.getEdgeSource() instanceof ProtobufOneOfField))
            .map(v -> (ProtobufField) v)
            .collect(Collectors.groupingBy(ProtobufField::parentMessageName));

    edgeSet().stream().filter(FieldTypeEdge::isMessageType).collect(Collectors.toSet()).stream()
        .map(e -> (ProtobufField) e.getEdgeTarget())
        .forEach(f -> attachNestedMessageFields(fieldMap, f));
  }

  private void addEnum(
      DescriptorProtos.FileDescriptorProto fileProto,
      DescriptorProtos.DescriptorProto messageProto) {
    messageProto
        .getEnumTypeList()
        .forEach(
            enumProto -> {
              ProtobufEnum enumVertex =
                  ProtobufEnum.enumBuilder()
                      .fileProto(fileProto)
                      .messageProto(messageProto)
                      .enumProto(enumProto)
                      .build();
              addVertex(enumVertex);
            });
  }

  private void addNestedMessage(
      DescriptorProtos.FileDescriptorProto fileProto,
      DescriptorProtos.DescriptorProto messageProto) {
    if (messageProto.getNestedTypeCount() < 1) {
      return;
    }

    messageProto
        .getNestedTypeList()
        .forEach(
            nestedMessageProto -> {
              ProtobufMessage nestedMessageVertex =
                  ProtobufMessage.builder()
                      .fileProto(fileProto)
                      .parentMessageProto(messageProto)
                      .messageProto(nestedMessageProto)
                      .build();
              addVertex(nestedMessageVertex);

              nestedMessageProto
                  .getFieldList()
                  .forEach(
                      nestedFieldProto -> {
                        ProtobufField field =
                            ProtobufField.builder()
                                .protobufMessage(nestedMessageVertex)
                                .fieldProto(nestedFieldProto)
                                .isNestedType(true)
                                .build();

                        // Add field vertex
                        addVertex(field);

                        // Add schema to field edge
                        if (!field.isMessage()) {
                          FieldTypeEdge.builder()
                              .edgeSource(nestedMessageVertex)
                              .edgeTarget(field)
                              .type(field.fieldPathType())
                              .build()
                              .inGraph(this);
                        }
                      });

              addNestedMessage(fileProto, nestedMessageProto);
            });
  }

  private Stream<ProtobufField> addOneOf(ProtobufMessage messageVertex, ProtobufField fieldVertex) {
    // Handle oneOf
    ProtobufField oneOfVertex =
        ProtobufOneOfField.oneOfBuilder()
            .protobufMessage(messageVertex)
            .fieldProto(fieldVertex.getFieldProto())
            .build();
    addVertex(oneOfVertex);

    FieldTypeEdge.builder()
        .edgeSource(messageVertex)
        .edgeTarget(oneOfVertex)
        .type(oneOfVertex.fieldPathType())
        .build()
        .inGraph(this);

    // Add oneOf field to field edge
    FieldTypeEdge.builder()
        .edgeSource(oneOfVertex)
        .edgeTarget(fieldVertex)
        .type(fieldVertex.fieldPathType())
        .isMessageType(fieldVertex.isMessage())
        .build()
        .inGraph(this);

    return Stream.of(oneOfVertex);
  }

  private Stream<ProtobufField> linkMessageToField(
      ProtobufMessage messageVertex, ProtobufField fieldVertex) {
    FieldTypeEdge.builder()
        .edgeSource(messageVertex)
        .edgeTarget(fieldVertex)
        .type(fieldVertex.fieldPathType())
        .isMessageType(fieldVertex.isMessage())
        .build()
        .inGraph(this);

    return Stream.of(fieldVertex);
  }

  private void attachNestedMessageFields(
      Map<String, List<ProtobufField>> fieldMap, ProtobufField messageField) {
    fieldMap
        .getOrDefault(messageField.nativeType(), List.of())
        .forEach(
            target -> {
              FieldTypeEdge.builder()
                  .edgeSource(messageField)
                  .edgeTarget(target)
                  .type(target.fieldPathType())
                  .isMessageType(target.isMessage())
                  .build()
                  .inGraph(this);
            });
  }

  private static final Set<String> GOOGLE_WRAPPERS =
      Set.of("google/protobuf/wrappers.proto", "google/protobuf/timestamp.proto");

  private void flattenGoogleWrapped() {
    HashSet<ProtobufElement> removeVertices = new HashSet<>();
    HashSet<FieldTypeEdge> removeEdges = new HashSet<>();
    HashSet<ProtobufElement> addVertices = new HashSet<>();
    HashSet<FieldTypeEdge> addEdges = new HashSet<>();

    Set<ProtobufElement> googleWrapped =
        vertexSet().stream()
            .filter(
                v ->
                    v instanceof ProtobufMessage
                        && GOOGLE_WRAPPERS.contains(v.fileProto().getName()))
            .collect(Collectors.toSet());
    removeVertices.addAll(googleWrapped);

    Set<ProtobufField> wrappedPrimitiveFields =
        googleWrapped.stream()
            .flatMap(wrapped -> outgoingEdgesOf(wrapped).stream())
            .map(FieldTypeEdge::getEdgeTarget)
            .map(ProtobufField.class::cast)
            .collect(Collectors.toSet());
    removeVertices.addAll(wrappedPrimitiveFields);

    wrappedPrimitiveFields.stream()
        .filter(fld -> fld.getNumber() == 1)
        .forEach(
            primitiveField -> {
              // remove incoming old edges to primitive
              removeEdges.addAll(incomingEdgesOf(primitiveField));

              Set<ProtobufField> originatingFields =
                  incomingEdgesOf(primitiveField).stream()
                      .map(FieldTypeEdge::getEdgeSource)
                      .filter(edgeSource -> !googleWrapped.contains(edgeSource))
                      .map(ProtobufField.class::cast)
                      .collect(Collectors.toSet());
              removeVertices.addAll(originatingFields);

              originatingFields.forEach(
                  originatingField -> {
                    // Replacement Field
                    ProtobufElement fieldVertex =
                        originatingField.toBuilder()
                            .fieldPathType(primitiveField.fieldPathType())
                            .schemaFieldDataType(primitiveField.schemaFieldDataType())
                            .isMessageType(false)
                            .build();
                    addVertices.add(fieldVertex);

                    // link source field parent directly to primitive
                    Set<FieldTypeEdge> incomingEdges = incomingEdgesOf(originatingField);
                    removeEdges.addAll(incomingEdgesOf(originatingField));
                    addEdges.addAll(
                        incomingEdges.stream()
                            .map(
                                oldEdge ->
                                    // Replace old edge with new edge to primitive
                                    FieldTypeEdge.builder()
                                        .edgeSource(oldEdge.getEdgeSource())
                                        .edgeTarget(fieldVertex)
                                        .type(primitiveField.fieldPathType())
                                        .isMessageType(false) // known primitive
                                        .build())
                            .collect(Collectors.toSet()));
                  });

              // remove old fields
              removeVertices.addAll(originatingFields);
            });

    // Remove edges
    removeAllEdges(removeEdges);
    // Remove vertices
    removeAllVertices(removeVertices);
    // Add vertices
    addVertices.forEach(this::addVertex);
    // Add edges
    addEdges.forEach(e -> e.inGraph(this));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ProtobufGraph that = (ProtobufGraph) o;

    return rootProtobufMessage.equals(that.rootProtobufMessage);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + rootProtobufMessage.hashCode();
    return result;
  }

  public String getHash() {
    return String.valueOf(super.hashCode());
  }
}
