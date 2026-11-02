package io.datahubproject.metadata.context;

/**
 * Process-wide search-template runtime carried on {@link SearchContext}. The implementation lives
 * in metadata-io because rendering returns an OpenSearch query. Null means templates are off.
 */
public interface SearchTemplateRuntime {}
