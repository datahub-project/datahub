package datahub.protobuf.visitors.field;

import static datahub.protobuf.ProtobufUtils.getFieldOptions;
import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.schema.SchemaField;
import com.linkedin.tag.TagProperties;
import com.linkedin.util.Pair;
import datahub.protobuf.model.FieldTypeEdge;
import datahub.protobuf.model.ProtobufElement;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.VisitContext;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jgrapht.GraphPath;

public class ProtobufExtensionFieldVisitor extends SchemaFieldVisitor {

  @Override
  public Stream<Pair<SchemaField, Double>> visitField(ProtobufField field, VisitContext context) {
    boolean isPrimaryKey =
        getFieldOptions(field.getFieldProto()).stream()
            .map(Pair::getKey)
            .anyMatch(fieldDesc -> fieldDesc.getName().matches("(?i).*primary_?key"));

    List<TagAssociation> tags = getTagAssociations(field, context);
    List<GlossaryTermAssociation> terms = getGlossaryTermAssociations(field, context);

    return context
        .streamAllPaths(field)
        .map(
            path ->
                Pair.of(
                    createSchemaField(field, context, path, isPrimaryKey, tags, terms),
                    context.calculateSortOrder(path, field)));
  }

  private SchemaField createSchemaField(
      ProtobufField field,
      VisitContext context,
      GraphPath<ProtobufElement, FieldTypeEdge> path,
      boolean isPrimaryKey,
      List<TagAssociation> tags,
      List<GlossaryTermAssociation> terms) {
    String description = createFieldDescription(field);

    return new SchemaField()
        .setFieldPath(context.getFieldPath(path))
        .setNullable(!isPrimaryKey)
        .setIsPartOfKey(isPrimaryKey)
        .setDescription(description)
        .setNativeDataType(field.nativeType())
        .setType(field.schemaFieldDataType())
        .setGlobalTags(new GlobalTags().setTags(new TagAssociationArray(tags)))
        .setGlossaryTerms(
            new GlossaryTerms()
                .setTerms(new GlossaryTermAssociationArray(terms))
                .setAuditStamp(context.getAuditStamp()));
  }

  private String createFieldDescription(ProtobufField field) {
    StringBuilder description = new StringBuilder(field.comment());

    if (field.isEnum()) {
      description.append("\n\n");
      Map<String, String> enumValuesWithComments = field.getEnumValuesWithComments();
      if (!enumValuesWithComments.isEmpty()) {
        appendEnumValues(description, field, enumValuesWithComments);
      }
    }

    return description.toString();
  }

  private void appendEnumValues(
      StringBuilder description, ProtobufField field, Map<String, String> enumValuesWithComments) {
    enumValuesWithComments.forEach(
        (name, comment) -> {
          field.getEnumValues().stream()
              .filter(v -> v.getName().equals(name))
              .findFirst()
              .ifPresent(
                  value -> {
                    description.append(String.format("%d: %s", value.getNumber(), name));
                    if (!comment.isEmpty()) {
                      description.append(" - ").append(comment);
                    }
                    description.append("\n");
                  });
        });
  }

  private List<TagAssociation> getTagAssociations(ProtobufField field, VisitContext context) {
    Stream<TagAssociation> fieldTags =
        ProtobufExtensionUtil.extractTagPropertiesFromOptions(
                getFieldOptions(field.getFieldProto()), context.getGraph().getRegistry())
            .map(tag -> new TagAssociation().setTag(new TagUrn(tag.getName())));

    Stream<TagAssociation> promotedTags =
        promotedTags(field, context)
            .map(tag -> new TagAssociation().setTag(new TagUrn(tag.getName())));

    return Stream.concat(fieldTags, promotedTags)
        .distinct()
        .sorted(Comparator.comparing(t -> t.getTag().getName()))
        .collect(Collectors.toList());
  }

  private List<GlossaryTermAssociation> getGlossaryTermAssociations(
      ProtobufField field, VisitContext context) {
    Stream<GlossaryTermAssociation> fieldTerms =
        ProtobufExtensionUtil.extractTermAssociationsFromOptions(
            getFieldOptions(field.getFieldProto()), context.getGraph().getRegistry());

    Stream<GlossaryTermAssociation> promotedTerms = promotedTerms(field, context);

    return Stream.concat(fieldTerms, promotedTerms)
        .distinct()
        .sorted(Comparator.comparing(a -> a.getUrn().getNameEntity()))
        .collect(Collectors.toList());
  }

  /**
   * Promote tags from nested message to field.
   *
   * @return tags
   */
  private Stream<TagProperties> promotedTags(ProtobufField field, VisitContext context) {
    if (field.isMessage()) {
      return context.getGraph().outgoingEdgesOf(field).stream()
          .flatMap(
              e ->
                  ProtobufExtensionUtil.extractTagPropertiesFromOptions(
                      getMessageOptions(e.getEdgeTarget().messageProto()),
                      context.getGraph().getRegistry()))
          .distinct();
    } else {
      return Stream.of();
    }
  }

  /**
   * Promote terms from nested message to field.
   *
   * @return terms
   */
  private Stream<GlossaryTermAssociation> promotedTerms(ProtobufField field, VisitContext context) {
    if (field.isMessage()) {
      return context.getGraph().outgoingEdgesOf(field).stream()
          .flatMap(
              e ->
                  ProtobufExtensionUtil.extractTermAssociationsFromOptions(
                      getMessageOptions(e.getEdgeTarget().messageProto()),
                      context.getGraph().getRegistry()))
          .distinct();
    } else {
      return Stream.of();
    }
  }
}
