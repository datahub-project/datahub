package com.datahub.authorization.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Configurable overlays for entity types that remain unrestricted when view authorization is
 * enabled.
 *
 * <p>When {@code authorization.view.enabled} is true, every entity type is subject to view checks
 * unless it is marked {@code viewUnrestricted: true} in the entity registry or appears in the
 * effective overlay list. Each of {@link #value}, {@link #add}, and {@link #remove} is a
 * comma-separated list of registry entity names. Production overlays live in {@code
 * application.yaml}; the lean baseline is owned by {@code entity-registry.yml}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class ViewUnrestrictedEntityTypes {

  /**
   * Optional full list of unrestricted entity types. When non-empty, replaces the entity-registry
   * {@code viewUnrestricted} baseline before {@link #add} / {@link #remove} apply. Empty means use
   * the registry baseline.
   */
  private String value;

  /** Comma-separated registry names to append to the effective list. */
  private String add;

  /** Comma-separated registry names to remove from the effective list. */
  private String remove;

  @JsonIgnore
  public boolean isEmpty() {
    return parseCsv(value).isEmpty() && parseCsv(add).isEmpty() && parseCsv(remove).isEmpty();
  }

  /**
   * Parses a comma-separated entity-type CSV into an ordered, case-folded, de-duplicated list.
   * First occurrence wins when the same type appears more than once (ignoring case).
   */
  public static List<String> parseCsv(String csv) {
    if (csv == null || csv.isBlank()) {
      return Collections.emptyList();
    }
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s.toLowerCase(Locale.ROOT))
        .collect(
            Collectors.collectingAndThen(
                Collectors.toCollection(LinkedHashSet::new), List::copyOf));
  }

  public List<String> parsedValue() {
    return parseCsv(value);
  }

  public List<String> parsedAdd() {
    return parseCsv(add);
  }

  public List<String> parsedRemove() {
    return parseCsv(remove);
  }
}
