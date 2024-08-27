package datahub.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A class that makes it easy to create new {@link MetadataChangeProposal} events
 *
 * @param <T>
 */
@Value
@Slf4j
@AllArgsConstructor
public class MetadataChangeProposalWrapper<T extends DataTemplate> {

  String entityType;
  String entityUrn;
  ChangeType changeType;
  T aspect;
  String aspectName;

  public interface EntityTypeStepBuilder {
    EntityUrnStepBuilder entityType(String entityType);
  }

  public interface EntityUrnStepBuilder {
    ChangeStepBuilder entityUrn(String entityUrn);

    ChangeStepBuilder entityUrn(Urn entityUrn);
  }

  public interface ChangeStepBuilder {
    AspectStepBuilder upsert();
  }

  public interface AspectStepBuilder {
    Build aspect(DataTemplate aspect);
  }

  public interface Build {
    MetadataChangeProposalWrapper build();

    Build aspectName(String aspectName);
  }

  public static class MetadataChangeProposalWrapperBuilder
      implements EntityUrnStepBuilder,
          EntityTypeStepBuilder,
          ChangeStepBuilder,
          AspectStepBuilder,
          Build {

    private String entityUrn;
    private String entityType;
    private ChangeType changeType;
    private String aspectName;
    private DataTemplate aspect;

    @Override
    public EntityUrnStepBuilder entityType(String entityType) {
      Objects.requireNonNull(entityType, "entityType cannot be null");
      this.entityType = entityType;
      return this;
    }

    @Override
    public ChangeStepBuilder entityUrn(String entityUrn) {
      Objects.requireNonNull(entityUrn, "entityUrn cannot be null");
      try {
        Urn.createFromString(entityUrn);
      } catch (URISyntaxException uie) {
        throw new EventValidationException("Failed to parse a valid entity urn", uie);
      }
      this.entityUrn = entityUrn;
      return this;
    }

    @Override
    public ChangeStepBuilder entityUrn(Urn entityUrn) {
      Objects.requireNonNull(entityUrn, "entityUrn cannot be null");
      this.entityUrn = entityUrn.toString();
      return this;
    }

    @Override
    public AspectStepBuilder upsert() {
      this.changeType = ChangeType.UPSERT;
      return this;
    }

    @Override
    public Build aspect(DataTemplate aspect) {
      this.aspect = aspect;
      // Try to guess the aspect name from the aspect
      Map<String, Object> schemaProps = aspect.schema().getProperties();
      if (schemaProps != null && schemaProps.containsKey("Aspect")) {
        Object aspectProps = schemaProps.get("Aspect");
        if (aspectProps != null && aspectProps instanceof Map) {
          Map aspectMap = (Map) aspectProps;
          String aspectName = (String) aspectMap.get("name");
          this.aspectName = aspectName;
          log.debug("Inferring aspectName as {}", aspectName);
        }
      }
      if (this.aspectName == null) {
        log.warn("Could not infer aspect name from aspect");
      }
      return this;
    }

    @Override
    public MetadataChangeProposalWrapper build() {
      try {
        Objects.requireNonNull(
            this.aspectName,
            "aspectName could not be inferred from provided aspect and was not explicitly provided as an override");
        return new MetadataChangeProposalWrapper(
            entityType, entityUrn, changeType, aspect, aspectName);
      } catch (Exception e) {
        throw new EventValidationException("Failed to create a metadata change proposal event", e);
      }
    }

    @Override
    public Build aspectName(String aspectName) {
      this.aspectName = aspectName;
      return this;
    }
  }

  public static MetadataChangeProposalWrapper create(
      Consumer<EntityTypeStepBuilder> builderConsumer) {
    MetadataChangeProposalWrapperBuilder builder = new MetadataChangeProposalWrapperBuilder();
    builderConsumer.accept(builder);
    return builder.build();
  }

  public static EntityTypeStepBuilder builder() {
    return new MetadataChangeProposalWrapperBuilder();
  }
}
