/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import datahub.protobuf.model.ProtobufGraph;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class DomainVisitorTest {

  @Test
  public void visitorTest() throws IOException {
    ProtobufGraph graph = getTestProtobufGraph("extended_protobuf", "messageA");

    DomainVisitor test = new DomainVisitor();

    assertEquals(
        Set.of(Urn.createFromTuple("domain", "engineering")),
        graph
            .accept(getVisitContextBuilder("extended_protobuf.MessageA"), List.of(test))
            .collect(Collectors.toSet()));
  }
}
