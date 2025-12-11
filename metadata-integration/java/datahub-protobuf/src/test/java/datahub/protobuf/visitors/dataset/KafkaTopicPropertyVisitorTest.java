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

import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class KafkaTopicPropertyVisitorTest {

  @Test
  public void visitorTest() throws IOException {
    KafkaTopicPropertyVisitor test = new KafkaTopicPropertyVisitor();
    assertEquals(
        List.of(
            new DatasetProperties()
                .setCustomProperties(new StringMap(Map.of("kafka_topic", "platform.topic")))),
        getTestProtobufGraph("protobuf", "messageA")
            .accept(getVisitContextBuilder("MessageB"), List.of(test))
            .collect(Collectors.toList()));
  }

  @Test
  public void visitorEmptyTest() throws IOException {
    KafkaTopicPropertyVisitor test = new KafkaTopicPropertyVisitor();
    assertEquals(
        Set.of(),
        getTestProtobufGraph("protobuf", "messageB")
            .accept(getVisitContextBuilder("MessageB"), List.of(test))
            .collect(Collectors.toSet()));
  }
}
