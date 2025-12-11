/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.visitors;

import datahub.protobuf.model.ProtobufElement;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.model.ProtobufMessage;
import java.util.stream.Stream;

public interface ProtobufModelVisitor<T> {
  default Stream<T> visitField(ProtobufField field, VisitContext context) {
    return visitElement(field, context);
  }

  default Stream<T> visitMessage(ProtobufMessage message, VisitContext context) {
    return visitElement(message, context);
  }

  default Stream<T> visitElement(ProtobufElement element, VisitContext context) {
    return Stream.of();
  }

  default Stream<T> visitGraph(VisitContext context) {
    return Stream.of();
  }
}
