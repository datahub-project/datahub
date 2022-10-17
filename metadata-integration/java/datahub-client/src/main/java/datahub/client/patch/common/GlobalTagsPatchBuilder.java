package datahub.client.patch.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.TagUrn;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class GlobalTagsPatchBuilder extends AbstractPatchBuilder<GlobalTagsPatchBuilder> {

  private static final String BASE_PATH = "/tags/";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";

  /**
   * The tag urn to perform the patch operation on
   */
  private TagUrn tag = null;
  private String context = null;

  public GlobalTagsPatchBuilder urn(TagUrn tag) {
    this.tag = tag;
    return this;
  }

  public GlobalTagsPatchBuilder context(String context) {
    this.context = context;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.op, this.targetEntityUrn, this.tag);
  }

  @Override
  protected String getPath() {
    return BASE_PATH + tag;
  }

  @Override
  protected JsonNode getValue() {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, tag.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    return value;
  }

  @Override
  protected String getAspectName() {
    return GLOBAL_TAGS_ASPECT_NAME;
  }
}
