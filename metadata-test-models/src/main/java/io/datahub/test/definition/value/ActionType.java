package io.datahub.test.definition.value;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * The type of a single Test action
 */
public enum ActionType {
  /**
   * Add tags
   */
  ADD_TAGS,
  /**
   * Remove tags
   */
  REMOVE_TAGS,
  /**
   * Add glossary terms
   */
  ADD_GLOSSARY_TERMS,
  /**
   * Remove glossary terms
   */
  REMOVE_GLOSSARY_TERMS,
  /**
   * Sets domain
   */
  SET_DOMAIN,
  /**
   * Unsets domains
   */
   UNSET_DOMAIN,
  /**
   * Adds domains
   */
  ADD_DOMAINS,
  /**
   * Removes domains
   */
  REMOVE_DOMAINS,
  /**
   * Adds owners
   */
  ADD_OWNERS,
  /**
   * Removes owners
   */
  REMOVE_OWNERS;

  private static final Map<String, ActionType> NAME_TO_ACTION_TYPE = Arrays.stream(
      ActionType.values())
      .collect(Collectors.toMap(val -> val.name().toLowerCase(), val -> val));

  /**
   * Retrieves an {@link ActionType} by a case-insensitive common name, or throws {@link IllegalArgumentException} if it cannot be found.
   */
  public static ActionType fromCommonName(@Nonnull final String commonName) {
    String lowerName = commonName.toLowerCase();
    if (NAME_TO_ACTION_TYPE.containsKey(lowerName)) {
      return NAME_TO_ACTION_TYPE.get(lowerName);
    }
    throw new IllegalArgumentException(String.format("Unsupported operator type with name %s provided", commonName));
  }

  ActionType() {
  }
}
