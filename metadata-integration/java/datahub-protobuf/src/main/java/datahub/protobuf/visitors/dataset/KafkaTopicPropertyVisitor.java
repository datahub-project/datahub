package datahub.protobuf.visitors.dataset;

import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class KafkaTopicPropertyVisitor implements ProtobufModelVisitor<DatasetProperties> {

  @Override
  public Stream<DatasetProperties> visitGraph(VisitContext context) {
    return getKafkaTopic(context.root().comment()).stream()
        .map(
            kafkaTopic ->
                new DatasetProperties()
                    .setCustomProperties(new StringMap(Map.of("kafka_topic", kafkaTopic))));
  }

  private static final Pattern TOPIC_NAME_REGEX =
      Pattern.compile("(?si).*kafka.+topic.+[`]([a-z._-]+)[`].*");

  private static Optional<String> getKafkaTopic(String text) {
    Matcher m = TOPIC_NAME_REGEX.matcher(text);
    return m.matches() ? Optional.of(m.group(1)) : Optional.empty();
  }
}
