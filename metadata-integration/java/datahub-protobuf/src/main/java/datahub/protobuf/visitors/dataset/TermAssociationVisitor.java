/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.linkedin.common.GlossaryTermAssociation;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.stream.Stream;

public class TermAssociationVisitor implements ProtobufModelVisitor<GlossaryTermAssociation> {

  @Override
  public Stream<GlossaryTermAssociation> visitGraph(VisitContext context) {
    return ProtobufExtensionUtil.extractTermAssociationsFromOptions(
        getMessageOptions(context.root().messageProto()), context.getGraph().getRegistry());
  }
}
