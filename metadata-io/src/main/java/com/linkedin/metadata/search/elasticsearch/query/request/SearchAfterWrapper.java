package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.*;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opensearch.search.SearchHit;

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

  @Nullable
  public static String nextScrollId(SearchHit[] searchHits, int requestedCount) {
    return nextScrollId(searchHits, requestedCount, null, 0L);
  }

  @Nullable
  public static String nextScrollId(
      SearchHit[] searchHits, int requestedCount, @Nullable String pitId, long expirationTime) {
    if (searchHits.length == requestedCount && searchHits.length > 0) {
      Object[] sort = searchHits[searchHits.length - 1].getSortValues();
      if (sort != null && sort.length > 0) {
        return new SearchAfterWrapper(sort, pitId, expirationTime).toScrollId();
      }
    }
    return null;
  }
}
