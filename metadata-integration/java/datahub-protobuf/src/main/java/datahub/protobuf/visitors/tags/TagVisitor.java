/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.visitors.tags;

import static datahub.protobuf.ProtobufUtils.getFieldOptions;
import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.tag.TagProperties;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.stream.Stream;

public class TagVisitor
    implements ProtobufModelVisitor<MetadataChangeProposalWrapper<? extends RecordTemplate>> {
  private static final String TAG_PROPERTIES_ASPECT = "tagProperties";

  @Override
  public Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> visitGraph(
      VisitContext context) {
    return ProtobufExtensionUtil.extractTagPropertiesFromOptions(
            getMessageOptions(context.root().messageProto()), context.getGraph().getRegistry())
        .map(TagVisitor::wrapTagProperty);
  }

  @Override
  public Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> visitField(
      ProtobufField field, VisitContext context) {
    return ProtobufExtensionUtil.extractTagPropertiesFromOptions(
            getFieldOptions(field.getFieldProto()), context.getGraph().getRegistry())
        .map(TagVisitor::wrapTagProperty);
  }

  private static MetadataChangeProposalWrapper<TagProperties> wrapTagProperty(
      TagProperties tagProperty) {
    return new MetadataChangeProposalWrapper<>(
        "tag",
        new TagUrn(tagProperty.getName()).toString(),
        ChangeType.UPSERT,
        tagProperty,
        TAG_PROPERTIES_ASPECT);
  }
}
