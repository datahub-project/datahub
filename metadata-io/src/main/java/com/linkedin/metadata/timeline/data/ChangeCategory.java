package com.linkedin.metadata.timeline.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public enum ChangeCategory {
  //description, institutionalmemory, properties docs, field level docs/description etc.
  DOCUMENTATION,
  //(field or top level) add term, remove term, etc.
  GLOSSARY_TERM,
  //add new owner, remove owner, change ownership type etc.
  OWNERSHIP,
  //new field, remove field, field type change,
  TECHNICAL_SCHEMA,
  //(field or top level) add tag, remove tag,
  TAG;

  public static final Map<List<String>, ChangeCategory> COMPOUND_CATEGORIES;

  // Add any compound categories here to support different cases in the API
  static {
    COMPOUND_CATEGORIES = new HashMap<>();
    COMPOUND_CATEGORIES.put(Arrays.asList(GLOSSARY_TERM.name().split("_")), GLOSSARY_TERM);
    COMPOUND_CATEGORIES.put(Arrays.asList(TECHNICAL_SCHEMA.name().split("_")), TECHNICAL_SCHEMA);
  }
}
