package io.datahubproject.models.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.common.urn.Urn;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class ContainerKey extends DataHubKey {
  private String platform;
  private String instance;

  private static final String URN_PREFIX = "urn:li:container:";
  private static final String URN_ENTITY = "container";
  private static final String PLATFORM_MAP_FIELD = "platform";
  private static final String INSTANCE_MAP_FIELD = "instance";

  @Override
  public Map<String, String> guidDict() {

    Map<String, String> bag = new HashMap<>();
    if (platform != null) bag.put(PLATFORM_MAP_FIELD, platform);
    if (instance != null) bag.put(INSTANCE_MAP_FIELD, instance);

    return bag;
  }

  public String asUrnString() {
    String guid = guid();
    return URN_PREFIX + guid;
  }

  public Urn asUrn() {
    return Urn.createFromTuple(URN_ENTITY, guid());
  }
}
