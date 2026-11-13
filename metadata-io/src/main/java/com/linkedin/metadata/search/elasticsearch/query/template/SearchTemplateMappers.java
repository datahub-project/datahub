package com.linkedin.metadata.search.elasticsearch.query.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

/** JSON for template DSL fragments. YAML is only for the shipped bootstrap file. */
public final class SearchTemplateMappers {
  public static final ObjectMapper JSON = new ObjectMapper();
  public static final YAMLMapper YAML = new YAMLMapper();

  private SearchTemplateMappers() {}
}
