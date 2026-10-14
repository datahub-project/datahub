package com.linkedin.metadata.search.elasticsearch.query.template;

/** GraphQL surface keys. A surface overrides only the intents it lists. */
public final class SearchTemplateSurfaces {
  public static final String SEARCH = "search";
  public static final String SEARCH_ACROSS_ENTITIES = "searchAcrossEntities";
  public static final String SCROLL = "scroll";
  public static final String AGGREGATE = "aggregate";
  public static final String BROWSE_V2 = "browseV2";
  public static final String SEARCH_ACROSS_LINEAGE = "searchAcrossLineage";

  private SearchTemplateSurfaces() {}
}
