package com.linkedin.gms.factory.graphql;

import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class GraphQLResponseObjectMapperFactoryTest {

  /**
   * The whole reason this mapper is separate from the primary one: it must serialize a null field
   * as an explicit JSON null (GraphQL shape), not omit it the way a NON_NULL-configured mapper
   * would.
   */
  @Test
  public void testResponseMapperSerializesExplicitNulls() throws Exception {
    ObjectMapper mapper = new GraphQLResponseObjectMapperFactory().graphQLResponseObjectMapper();

    Map<String, Object> value = new HashMap<>();
    value.put("present", "v");
    value.put("missing", null);

    JsonNode parsed = mapper.readTree(mapper.writeValueAsString(value));
    assertTrue(parsed.has("missing"), "null field must be present as an explicit JSON null");
    assertTrue(parsed.get("missing").isNull());
  }
}
