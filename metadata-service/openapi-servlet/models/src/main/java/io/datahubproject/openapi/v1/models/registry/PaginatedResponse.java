package io.datahubproject.openapi.v1.models.registry;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(Include.NON_NULL)
public class PaginatedResponse<T> {
  private List<T> elements;
  private Integer start;
  private Integer count;
  private Integer total;

  /** Creates a paginated response for DTOs that need custom mapping */
  public static <K extends Comparable<K>, V, D> PaginatedResponse<D> fromMap(
      Map<K, V> items,
      Integer start,
      Integer count,
      java.util.function.Function<Map.Entry<K, V>, D> mapper) {

    if (items == null || items.isEmpty()) {
      return PaginatedResponse.<D>builder().elements(List.of()).start(0).count(0).total(0).build();
    }

    // Sort by key and map to DTOs
    List<D> sortedItems =
        items.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(mapper)
            .collect(Collectors.toList());

    // Apply pagination
    int actualStart = start != null ? start : 0;
    int actualCount = count != null ? count : sortedItems.size();

    // Ensure start is within bounds
    if (actualStart > (sortedItems.size() - 1)) {
      throw new IllegalArgumentException(
          String.format("Start offset %s exceeds total %s", actualStart, sortedItems.size() - 1));
    }

    // Calculate end index
    int endIndex = Math.min(actualStart + actualCount, sortedItems.size());

    // Get the page
    List<D> pageItems = sortedItems.subList(actualStart, endIndex);

    return PaginatedResponse.<D>builder()
        .elements(pageItems)
        .start(actualStart)
        .count(pageItems.size())
        .total(sortedItems.size())
        .build();
  }
}
