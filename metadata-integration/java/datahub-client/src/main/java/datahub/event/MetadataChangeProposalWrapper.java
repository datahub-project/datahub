package datahub.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;


/**
 * A class that makes it easy to create new {@link MetadataChangeProposal} events
 * @param <T>
 */
@Data
@Builder
@Slf4j
public class MetadataChangeProposalWrapper<T extends DataTemplate> {

  @NonNull
  String entityType;
  @NonNull
  String entityUrn;
  @Builder.Default
  ChangeType changeType = ChangeType.UPSERT;
  T aspect;
  String aspectName;

  /**
   * Validates that this class is well formed.
   * Mutates the class to auto-fill
   * @throws EventValidationException is the event is not valid
   */
  protected static void validate(MetadataChangeProposalWrapper mcpw) throws EventValidationException {
    try {
      Urn.createFromString(mcpw.entityUrn);
    } catch (URISyntaxException uie) {
      throw new EventValidationException("Failed to parse a valid entity urn", uie);
    }

    if (mcpw.getAspect() != null && mcpw.getAspectName() == null) {
      // Try to guess the aspect name from the aspect
      Map<String, Object> schemaProps = mcpw.getAspect().schema().getProperties();
      if (schemaProps != null && schemaProps.containsKey("Aspect")) {
        Object aspectProps = schemaProps.get("Aspect");
        if (aspectProps != null && aspectProps instanceof Map) {
          Map aspectMap = (Map) aspectProps;
          String aspectName = (String) aspectMap.get("name");
          mcpw.setAspectName(aspectName);
          log.debug("Inferring aspectName as {}", aspectName);
        }
      }
      if (mcpw.getAspectName() == null) {
        throw new EventValidationException("Aspect name was null and could not be inferred.");
      }
    }
    if (mcpw.getChangeType() != ChangeType.UPSERT) {
      throw new EventValidationException("Change type other than UPSERT is not supported at this time. Supplied " + mcpw.getChangeType());
    }
    if (mcpw.getChangeType() == ChangeType.UPSERT && mcpw.getAspect() == null) {
      throw new EventValidationException("Aspect cannot be null if ChangeType is UPSERT");
    }

  }


  public static MetadataChangeProposalWrapper create(Consumer<ValidatingMCPWBuilder> builderConsumer) {
    return new ValidatingMCPWBuilder().with(builderConsumer).build();
  }


  public static MetadataChangeProposalWrapperBuilder builder() {
    return new ValidatingMCPWBuilder();
  }

  public static class ValidatingMCPWBuilder extends MetadataChangeProposalWrapperBuilder {

    @Override
    public MetadataChangeProposalWrapper build() {
      MetadataChangeProposalWrapper mcpw = super.build();
      validate(mcpw);
      return mcpw;
    }

    public ValidatingMCPWBuilder with(Consumer<ValidatingMCPWBuilder> builderConsumer) {
      builderConsumer.accept(this);
      return this;
    }


  }

}
