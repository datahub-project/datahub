package com.linkedin.metadata.config.search;

import java.util.List;
import lombok.Data;

/**
 * Tuning for the default autocomplete (typeahead) query built by {@code
 * AutocompleteRequestHandler}. Production values live in {@code application.yaml} under {@code
 * elasticsearch.search.autocomplete}.
 */
@Data
public class AutocompleteQueryConfiguration {
  /**
   * Entities whose typeahead requires EVERY typed token to prefix-match (one MUST per token) once
   * two or more tokens are typed. Meant for people pickers (owners filter, add owners, assignees):
   * for "John K", "Johnathan Killroy" is returned and "John Fitzgerald" is not, instead of the
   * whole-term match ranking first. Case-insensitive on the entity name; an empty list disables the
   * behaviour. Single-token queries are unchanged.
   */
  private List<String> allTokensMustPrefixMatchEntities = List.of("corpuser", "corpGroup");
}
