package datahub.client.patch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.http.entity.ContentType;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public abstract class AbstractPatchBuilder<T extends AbstractPatchBuilder<T>> {

  protected String op = null;
  protected Urn targetEntityUrn = null;

  public static final String OP_KEY = "op";
  public static final String VALUE_KEY = "value";
  public static final String PATH_KEY = "path";

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Builder method
   * @return a {@link MetadataChangeProposal} constructed from the builder's properties
   */
  public MetadataChangeProposal build() {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setChangeType(ChangeType.PATCH);
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setEntityUrn(this.targetEntityUrn);
    proposal.setAspectName(getAspectName());
    proposal.setAspect(buildPatch());
    return proposal;
  }

  /**
   * Generates Json patch for this builder to be set on the returned proposal from the buidler method
   * @return a JsonPatch wrapped by GenericAspect
   */
  protected GenericAspect buildPatch() {
    boolean propertyNotSet = getRequiredProperties().anyMatch(Objects::isNull);
    if (propertyNotSet) {
      throw new IllegalArgumentException("Required property not set: " + this);
    }

    ObjectNode patch = instance.objectNode();
    patch.put(OP_KEY, this.op)
        .put(PATH_KEY, getPath())
        .put(VALUE_KEY, getValue());

    ArrayNode patches = instance.arrayNode();
    patches.add(patch);

    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType(ContentType.APPLICATION_JSON.getMimeType());
    genericAspect.setValue(ByteString.copyString(patches.toString(), StandardCharsets.UTF_8));

    return genericAspect;
  }

  /**
   * Properties required by this builder to properly construct a patch
   * @return a list of properties to execute a null check on
   */
  protected abstract Stream<Object> getRequiredProperties();

  /**
   * Path of the patch to apply, patch builders with complex object subpath will use a FieldPath enum to specify
   * @return path string
   */
  protected abstract String getPath();

  /**
   * Json Patch value in JsonNode format, Note: field values initialize as null, so setting a field to null will be
   * ignored rather than actually setting the field to null.
   * @return {@link JsonNode}
   */
  protected abstract JsonNode getValue();

  /**
   * The aspect name associated with this builder
   * @return aspect name
   */
  protected abstract String getAspectName();

  /**
   * Sets the operation of the patch
   * @param op The op value
   * @return this PatchBuilder subtype's instance
   */
  @SuppressWarnings("unchecked")
  public T op(PatchOperationType op) {
    this.op = op.getValue();
    return (T) this;
  }

  /**
   * Sets the target entity urn to be updated by this patch
   * @param urn The target entity whose aspect is to be patched by this update
   * @return this PatchBuilder subtype's instance
   */
  @SuppressWarnings("unchecked")
  public T urn(Urn urn) {
    this.targetEntityUrn = urn;
    return (T) this;
  }

}
