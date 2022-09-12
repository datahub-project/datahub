package datahub.client.patch.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class OwnershipPatchBuilder extends AbstractPatchBuilder<OwnershipPatchBuilder> {

  private static final String BASE_PATH = "/owners/";
  private static final String OWNER_KEY = "owner";
  private static final String TYPE_KEY = "type";

  /**
   * The owner to update with this patch operation
   */
  private Urn owner = null;
  private OwnershipType type = null;

  public OwnershipPatchBuilder owner(Urn owner) {
    this.owner = owner;
    return this;
  }

  public OwnershipPatchBuilder ownershipType(OwnershipType type) {
    this.type = type;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.owner, this.type, this.op, this.targetEntityUrn);
  }

  @Override
  protected String getPath() {
    return BASE_PATH + owner.toString() + "/" + type.toString();
  }

  @Override
  protected JsonNode getValue() {
    ObjectNode value = instance.objectNode();
    value.put(OWNER_KEY, owner.toString());

    if (type != null) {
      value.put(TYPE_KEY, type.toString());
    }

    return value;
  }

  @Override
  protected String getAspectName() {
    return OWNERSHIP_ASPECT_NAME;
  }
}
