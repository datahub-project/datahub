package datahub.client;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.events.metadata.ChangeType;
import lombok.Builder;
import lombok.Value;


/**
 * A class that makes it easy to create new {@link MetadataChangeProposal} events
 * @param <T>
 */
@JsonDeserialize(builder = MetadataChangeProposalWrapper.MetadataChangeProposalWrapperBuilder.class)
@Value
@Builder
public class MetadataChangeProposalWrapper<T extends DataTemplate> {

  String entityType;
  Urn entityUrn;
  ChangeType changeType;
  T aspect;
  String aspectName;

  @JsonPOJOBuilder(withPrefix = "")
  public static class MetadataChangeProposalWrapperBuilder<T extends DataTemplate> {
  }

}
