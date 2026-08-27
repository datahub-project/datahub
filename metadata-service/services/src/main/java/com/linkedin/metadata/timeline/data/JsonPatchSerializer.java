package com.linkedin.metadata.timeline.data;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import jakarta.json.JsonPatch;
import java.io.IOException;

/**
 * Serializes a {@link JsonPatch} as its RFC-6902 JSON array form. The default parsson
 * implementation ({@code org.eclipse.parsson.JsonPatchImpl}) is not a Jackson bean, so Jackson
 * would otherwise throw "No serializer found" and fail the entire timeline response when {@code
 * raw=true} is requested.
 */
public class JsonPatchSerializer extends JsonSerializer<JsonPatch> {
  @Override
  public void serialize(JsonPatch value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeRawValue(value.toJsonArray().toString());
  }
}
