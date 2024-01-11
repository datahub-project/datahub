package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.models.registry.template.util.TemplateUtil.*;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchAfterWrapper implements Serializable {
  private Object[] sort;
  private String pitId;
  private long expirationTime;

  public static SearchAfterWrapper fromScrollId(String scrollId) {
    try {
      return OBJECT_MAPPER.readValue(
          Base64.getDecoder().decode(scrollId.getBytes(StandardCharsets.UTF_8)),
          SearchAfterWrapper.class);
    } catch (IOException e) {
      throw new IllegalStateException("Invalid scroll Id cannot be mapped: " + scrollId, e);
    }
  }

  public String toScrollId() {
    try {
      return Base64.getEncoder().encodeToString(OBJECT_MAPPER.writeValueAsBytes(this));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to encode SearchAfterWrapper as scrollId: " + this);
    }
  }
}
