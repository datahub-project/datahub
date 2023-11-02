package datahub.client.patch.monitor;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorStatus;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import datahub.client.patch.common.CustomPropertiesPatchBuilder;
import datahub.client.patch.subtypesupport.CustomPropertiesPatchBuilderSupport;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.MONITOR_ENTITY_NAME;
import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;


public class MonitorInfoPatchBuilder extends AbstractMultiFieldPatchBuilder<MonitorInfoPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<MonitorInfoPatchBuilder> {

  public static final String BASE_PATH = "/";
  public static final String ASSERTION_STATUS_KEY = "status";
  public static final String ASSERTION_MONITOR_KEY = "assertionMonitor";

  private CustomPropertiesPatchBuilder<MonitorInfoPatchBuilder> customPropertiesPatchBuilder = new CustomPropertiesPatchBuilder<>(this);

  public MonitorInfoPatchBuilder setStatus(@Nonnull MonitorStatus status) {
    ObjectNode statusNode = instance.objectNode();
    statusNode.put("mode", status.getMode().toString());
    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), BASE_PATH + ASSERTION_STATUS_KEY, statusNode));
    return this;
  }

  public MonitorInfoPatchBuilder setAssertionMonitor(@Nonnull AssertionMonitor assertionMonitor) {
    try {
      ObjectNode assertionMonitorNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(assertionMonitor));
      pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), BASE_PATH + ASSERTION_MONITOR_KEY, assertionMonitorNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to set Assertion Monitor, failed to parse provided aspect json", e);
    }
  }

  @Override
  protected List<ImmutableTriple<String, String, JsonNode>> getPathValues() {
    pathValues.addAll(customPropertiesPatchBuilder.getSubPaths());
    return pathValues;
  }

  @Override
  protected String getAspectName() {
    return MONITOR_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return MONITOR_ENTITY_NAME;
  }

  @Override
  public MonitorInfoPatchBuilder addCustomProperty(@Nonnull String key, @Nonnull String value) {
    customPropertiesPatchBuilder.addProperty(key, value);
    return this;
  }

  @Override
  public MonitorInfoPatchBuilder removeCustomProperty(@Nonnull String key) {
    customPropertiesPatchBuilder.removeProperty(key);
    return this;
  }

  @Override
  public MonitorInfoPatchBuilder setCustomProperties(Map<String, String> properties) {
    customPropertiesPatchBuilder.setProperties(properties);
    return this;
  }
}

