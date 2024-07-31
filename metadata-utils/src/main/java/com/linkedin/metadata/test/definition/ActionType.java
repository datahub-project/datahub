package com.linkedin.metadata.test.definition;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** The type of a single Test action */
public enum ActionType {
  /** Add tags */
  ADD_TAGS,
  /** Remove tags */
  REMOVE_TAGS,
  /** Add glossary terms */
  ADD_GLOSSARY_TERMS,
  /** Remove glossary terms */
  REMOVE_GLOSSARY_TERMS,
  /** Sets domain */
  SET_DOMAIN,
  /** Unsets domains */
  UNSET_DOMAIN,
  /** Adds domains */
  ADD_DOMAINS,
  /** Removes domains */
  REMOVE_DOMAINS,
  /** Adds owners */
  ADD_OWNERS,
  /** Removes owners */
  REMOVE_OWNERS,
  /** Deprecate item */
  DEPRECATE,
  /** Un Deprecate item */
  UN_DEPRECATE,
  /** Assign a requirements form */
  ASSIGN_FORM,
  /** Unassign a requirements form */
  UNASSIGN_FORM,
  /** Unset a particular prompt inside a requirements form when a requirement is no longer met. */
  SET_FORM_PROMPT_INCOMPLETE,
  /** Submits a response to a form prompt */
  SUBMIT_FORM_PROMPT,
  /** Verifies a form for given entities */
  VERIFY_FORM;

  private static final Map<String, ActionType> NAME_TO_ACTION_TYPE =
      Arrays.stream(ActionType.values())
          .collect(Collectors.toMap(val -> val.name().toLowerCase(), val -> val));

  /**
   * Retrieves an {@link ActionType} by a case-insensitive common name, or throws {@link
   * IllegalArgumentException} if it cannot be found.
   */
  public static ActionType fromCommonName(@Nonnull final String commonName) {
    String lowerName = commonName.toLowerCase();
    if (NAME_TO_ACTION_TYPE.containsKey(lowerName)) {
      return NAME_TO_ACTION_TYPE.get(lowerName);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported operator type with name %s provided", commonName));
  }

  ActionType() {}
}
