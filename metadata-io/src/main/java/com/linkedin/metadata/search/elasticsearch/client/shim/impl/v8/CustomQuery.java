package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBase;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import co.elastic.clients.elasticsearch._types.query_dsl.SimpleQueryStringQuery;
import co.elastic.clients.json.JsonpDeserializable;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.ObjectBuilderDeserializer;
import co.elastic.clients.json.ObjectDeserializer;
import co.elastic.clients.util.ObjectBuilder;
import java.util.function.Function;

@JsonpDeserializable
public class CustomQuery extends Query {

  // Modified SimpleQueryStringQuery deser with customized flag deser
  private static void setupSimpleQueryStringQueryDeserializer(
      ObjectDeserializer<SimpleQueryStringQuery.Builder> op) {
    op.add(QueryBase.AbstractBuilder::boost, JsonpDeserializer.floatDeserializer(), "boost");
    op.add(QueryBase.AbstractBuilder::queryName, JsonpDeserializer.stringDeserializer(), "_name");
    op.add(
        SimpleQueryStringQuery.Builder::analyzer,
        JsonpDeserializer.stringDeserializer(),
        "analyzer");
    op.add(
        SimpleQueryStringQuery.Builder::analyzeWildcard,
        JsonpDeserializer.booleanDeserializer(),
        "analyze_wildcard");
    op.add(
        SimpleQueryStringQuery.Builder::autoGenerateSynonymsPhraseQuery,
        JsonpDeserializer.booleanDeserializer(),
        "auto_generate_synonyms_phrase_query");
    op.add(
        SimpleQueryStringQuery.Builder::defaultOperator,
        Operator._DESERIALIZER,
        "default_operator");
    op.add(
        SimpleQueryStringQuery.Builder::fields,
        JsonpDeserializer.arrayDeserializer(JsonpDeserializer.stringDeserializer()),
        "fields");
    op.add(SimpleQueryStringQuery.Builder::flags, new SimpleQueryStringFlagDeserializer(), "flags");
    op.add(
        SimpleQueryStringQuery.Builder::fuzzyMaxExpansions,
        JsonpDeserializer.integerDeserializer(),
        "fuzzy_max_expansions");
    op.add(
        SimpleQueryStringQuery.Builder::fuzzyPrefixLength,
        JsonpDeserializer.integerDeserializer(),
        "fuzzy_prefix_length");
    op.add(
        SimpleQueryStringQuery.Builder::fuzzyTranspositions,
        JsonpDeserializer.booleanDeserializer(),
        "fuzzy_transpositions");
    op.add(
        SimpleQueryStringQuery.Builder::lenient,
        JsonpDeserializer.booleanDeserializer(),
        "lenient");
    op.add(
        SimpleQueryStringQuery.Builder::minimumShouldMatch,
        JsonpDeserializer.stringDeserializer(),
        "minimum_should_match");
    op.add(SimpleQueryStringQuery.Builder::query, JsonpDeserializer.stringDeserializer(), "query");
    op.add(
        SimpleQueryStringQuery.Builder::quoteFieldSuffix,
        JsonpDeserializer.stringDeserializer(),
        "quote_field_suffix");
  }

  public CustomQuery(QueryVariant value) {
    super(value);
  }

  protected static void setupCustomQueryDeserializer(ObjectDeserializer<Builder> op) {
    setupQueryDeserializer(op);
    op.add(
        Builder::simpleQueryString,
        ObjectBuilderDeserializer.lazy(
            SimpleQueryStringQuery.Builder::new,
            CustomQuery::setupSimpleQueryStringQueryDeserializer,
            SimpleQueryStringQuery.Builder::build),
        "simple_query_string");
  }

  public static Query ofCustom(Function<Builder, ObjectBuilder<Query>> fn) {
    return fn.apply(new CustomQueryBuilder()).build();
  }

  public static class CustomQueryBuilder extends Query.Builder {}

  public static final JsonpDeserializer<Query> _DESERIALIZER =
      ObjectBuilderDeserializer.lazy(
          Builder::new, CustomQuery::setupCustomQueryDeserializer, Builder::build);
}
