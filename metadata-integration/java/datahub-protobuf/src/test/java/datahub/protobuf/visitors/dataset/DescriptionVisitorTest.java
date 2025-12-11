/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.TestFixtures.*;
import static org.testng.Assert.assertEquals;

import datahub.protobuf.model.ProtobufGraph;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class DescriptionVisitorTest {

  @Test
  public void visitorTest() throws IOException {
    ProtobufGraph graph = getTestProtobufGraph("protobuf", "messageC2", "protobuf.MessageC2");

    DescriptionVisitor test = new DescriptionVisitor();

    assertEquals(
        Set.of("This contains nested type\n\nDescription for MessageC2"),
        graph
            .accept(getVisitContextBuilder("protobuf.MessageC2"), List.of(test))
            .collect(Collectors.toSet()));
  }
}
