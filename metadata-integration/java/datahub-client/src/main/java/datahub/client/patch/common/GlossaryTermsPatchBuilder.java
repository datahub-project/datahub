package datahub.client.patch.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.GlossaryTermUrn;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class GlossaryTermsPatchBuilder extends AbstractPatchBuilder<GlossaryTermsPatchBuilder> {

  private static final String BASE_PATH = "/glossaryTerms/";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";

  /**
   * The glossary term urn to perform the patch operation on
   */
  private GlossaryTermUrn urn = null;
  private String context = null;

  public GlossaryTermsPatchBuilder urn(GlossaryTermUrn urn) {
    this.urn = urn;
    return this;
  }

  public GlossaryTermsPatchBuilder context(String context) {
    this.context = context;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.op, this.targetEntityUrn, this.urn);
  }

  @Override
  protected String getPath() {
    return BASE_PATH + urn;
  }

  @Override
  protected JsonNode getValue() {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, urn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    return value;
  }

  @Override
  protected String getAspectName() {
    return GLOSSARY_TERMS_ASPECT_NAME;
  }
}
