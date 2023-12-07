package datahub.client.patch;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.http.entity.ContentType;

public abstract class AbstractMultiFieldPatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>> {

  public static final String OP_KEY = "op";
  public static final String VALUE_KEY = "value";
  public static final String PATH_KEY = "path";

  protected List<ImmutableTriple<String, String, JsonNode>> pathValues = new ArrayList<>();
  protected Urn targetEntityUrn = null;

  /**
   * Builder method
   *
   * @return a {@link MetadataChangeProposal} constructed from the builder's properties
   */
  public MetadataChangeProposal build() {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setChangeType(ChangeType.PATCH);
    proposal.setEntityType(getEntityType());
    proposal.setEntityUrn(this.targetEntityUrn);
    proposal.setAspectName(getAspectName());
    proposal.setAspect(buildPatch());
    return proposal;
  }

  /**
   * Sets the target entity urn to be updated by this patch
   *
   * @param urn The target entity whose aspect is to be patched by this update
   * @return this PatchBuilder subtype's instance
   */
  @SuppressWarnings("unchecked")
  public T urn(Urn urn) {
    this.targetEntityUrn = urn;
    return (T) this;
  }

  /**
   * The aspect name associated with this builder
   *
   * @return aspect name
   */
  protected abstract String getAspectName();

  /**
   * Returns the String representation of the Entity type associated with this aspect
   *
   * @return entity type name
   */
  protected abstract String getEntityType();

  /**
   * Overrides basic behavior to construct multiple patches based on properties
   *
   * @return a JsonPatch wrapped by GenericAspect
   */
  protected GenericAspect buildPatch() {
    if (pathValues.isEmpty()) {
      throw new IllegalArgumentException("No patches specified.");
    }

    ArrayNode patches = instance.arrayNode();
    List<ImmutableTriple<String, String, JsonNode>> triples = getPathValues();
    triples.forEach(
        triple ->
            patches.add(
                instance
                    .objectNode()
                    .put(OP_KEY, triple.left)
                    .put(PATH_KEY, triple.middle)
                    .set(VALUE_KEY, triple.right)));

    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType(ContentType.APPLICATION_JSON.getMimeType());
    genericAspect.setValue(ByteString.copyString(patches.toString(), StandardCharsets.UTF_8));

    return genericAspect;
  }

  /**
   * Constructs a list of Op, Path, Value triples to create as patches. Not idempotent and should
   * not be called more than once
   *
   * @return list of patch precursor triples
   */
  protected List<ImmutableTriple<String, String, JsonNode>> getPathValues() {
    return this.pathValues;
  }
}
