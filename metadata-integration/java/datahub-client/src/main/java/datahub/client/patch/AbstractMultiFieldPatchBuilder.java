package datahub.client.patch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linkedin.data.ByteString;
import com.linkedin.mxe.GenericAspect;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.http.entity.ContentType;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;


public abstract class AbstractMultiFieldPatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>> extends AbstractPatchBuilder<T> {

  /**
   * Overrides basic behavior to construct multiple patches based on properties
   * @return a JsonPatch wrapped by GenericAspect
   */
  @Override
  protected GenericAspect buildPatch() {
    boolean propertyNotSet = getRequiredProperties().anyMatch(Objects::isNull);
    if (propertyNotSet) {
      throw new IllegalArgumentException("Required property not set: " + this);
    }

    ArrayNode patches = instance.arrayNode();
    List<ImmutableTriple<String, String, JsonNode>> triples = getPathValues();
    triples.forEach(triple -> patches.add(instance.objectNode().put(OP_KEY, triple.left)
        .put(PATH_KEY, triple.middle)
        .set(VALUE_KEY, triple.right)));

    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType(ContentType.APPLICATION_JSON.getMimeType());
    genericAspect.setValue(ByteString.copyString(patches.toString(), StandardCharsets.UTF_8));

    return genericAspect;
  }

  /**
   * No-op, unused for this subtype. See getPathValues
   */
  @Override
  protected String getPath() {
    return null;
  }

  /**
   * No-op, unused for this subtype. See getPathValues
   */
  @Override
  protected JsonNode getValue() {
    return null;
  }

  /**
   * Constructs a list of Op, Path, Value triples to create as patches
   * @return list of patch precursor triples
   */
  protected abstract List<ImmutableTriple<String, String, JsonNode>> getPathValues();
}
