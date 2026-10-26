package com.datahub.gms.servlet;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_ANALYZER;

import com.datahub.gms.util.CSVWriter;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.EntityTypeUtils;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.WeightBuilder;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

@Slf4j
public class ConfigSearchExport extends HttpServlet {

  private static ConfigurationProvider getConfigProvider(WebApplicationContext ctx) {
    return (ConfigurationProvider) ctx.getBean("configurationProvider");
  }

  private static OperationContext getOperationContext(WebApplicationContext ctx) {
    return (OperationContext) ctx.getBean("systemOperationContext");
  }

  private static QueryFilterRewriteChain getQueryFilterRewriteChain(WebApplicationContext ctx) {
    return ctx.getBean(QueryFilterRewriteChain.class);
  }

  private void writeSearchCsv(WebApplicationContext ctx, PrintWriter pw) {
    OperationContext systemOpContext = getOperationContext(ctx);
    ConfigurationProvider configurationProvider = getConfigProvider(ctx);
    ElasticSearchConfiguration searchConfiguration = configurationProvider.getElasticSearch();
    EntityRegistry entityRegistry = systemOpContext.getEntityRegistry();
    QueryFilterRewriteChain queryFilterRewriteChain = getQueryFilterRewriteChain(ctx);
    SearchServiceConfiguration searchServiceConfiguration =
        configurationProvider.getSearchService();

    CSVWriter writer = CSVWriter.builder().printWriter(pw).build();

    String[] header = {
      "entity",
      "query_category",
      "match_category",
      "query_type",
      "field_name",
      "field_weight",
      "search_analyzer",
      "case_insensitive",
      "query_boost",
      "raw"
    };
    writer.println(header);

    List<String> searchableEntityNames =
        Optional.ofNullable(systemOpContext.getSearchContext().getDefaultSearchEntityNames())
            .orElseGet(
                () ->
                    EntityTypeUtils.resolve(
                        searchConfiguration.getSearch().getDefaultEntityTypes(), entityRegistry));

    searchableEntityNames.stream()
        .map(
            entityName -> {
              try {
                EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
                return Optional.of(entitySpec);
              } catch (IllegalArgumentException e) {
                log.warn("Failed to resolve entity `{}`", entityName);
                return Optional.<EntitySpec>empty();
              }
            })
        .filter(Optional::isPresent)
        .forEach(
            entitySpecOpt -> {
              EntitySpec entitySpec = entitySpecOpt.get();
              SearchRequest searchRequest =
                  SearchRequestHandler.getBuilder(
                          systemOpContext,
                          entitySpec,
                          searchConfiguration,
                          null,
                          queryFilterRewriteChain,
                          searchServiceConfiguration)
                      .getSearchRequest(
                          systemOpContext.withSearchFlags(
                              flags ->
                                  flags
                                      .setFulltext(true)
                                      .setSkipHighlighting(true)
                                      .setSkipAggregates(true)),
                          "*",
                          null,
                          null,
                          0,
                          0,
                          List.of());

              FunctionScoreQueryBuilder rankingQuery =
                  ((FunctionScoreQueryBuilder)
                      ((BoolQueryBuilder) searchRequest.source().query()).must().get(0));
              writeRelevancyRows(writer, entitySpec, rankingQuery.query());

              for (FunctionScoreQueryBuilder.FilterFunctionBuilder ffb :
                  rankingQuery.filterFunctionBuilders()) {
                if (ffb.getFilter() instanceof MatchAllQueryBuilder) {
                  MatchAllQueryBuilder filter = (MatchAllQueryBuilder) ffb.getFilter();

                  if (ffb.getScoreFunction() instanceof WeightBuilder) {
                    WeightBuilder scoreFunction = (WeightBuilder) ffb.getScoreFunction();
                    String[] row = {
                      entitySpec.getName(),
                      "score",
                      filter.getClass().getSimpleName(),
                      scoreFunction.getClass().getSimpleName(),
                      "*",
                      String.valueOf(scoreFunction.getWeight()),
                      "",
                      "true",
                      String.valueOf(filter.boost()),
                      String.format(
                              "{\"filter\":%s,\"scoreFunction\":%s",
                              filter, CSVWriter.builderToString(scoreFunction))
                          .replaceAll("\n", "")
                    };
                    writer.println(row);
                  } else if (ffb.getScoreFunction() instanceof FieldValueFactorFunctionBuilder) {
                    FieldValueFactorFunctionBuilder scoreFunction =
                        (FieldValueFactorFunctionBuilder) ffb.getScoreFunction();
                    String[] row = {
                      entitySpec.getName(),
                      "score",
                      filter.getClass().getSimpleName(),
                      scoreFunction.getClass().getSimpleName(),
                      scoreFunction.fieldName(),
                      String.valueOf(scoreFunction.factor()),
                      "",
                      "true",
                      String.valueOf(filter.boost()),
                      String.format(
                              "{\"filter\":%s,\"scoreFunction\":%s",
                              filter, CSVWriter.builderToString(scoreFunction))
                          .replaceAll("\n", "")
                    };
                    writer.println(row);
                  } else {
                    throw new IllegalStateException(
                        "Unhandled score function: " + ffb.getScoreFunction());
                  }
                } else if (ffb.getFilter() instanceof TermQueryBuilder) {
                  TermQueryBuilder filter = (TermQueryBuilder) ffb.getFilter();

                  if (ffb.getScoreFunction() instanceof WeightBuilder) {
                    WeightBuilder scoreFunction = (WeightBuilder) ffb.getScoreFunction();
                    String[] row = {
                      entitySpec.getName(),
                      "score",
                      filter.getClass().getSimpleName(),
                      scoreFunction.getClass().getSimpleName(),
                      filter.fieldName() + "=" + filter.value().toString(),
                      String.valueOf(scoreFunction.getWeight()),
                      KEYWORD_ANALYZER,
                      String.valueOf(filter.caseInsensitive()),
                      String.valueOf(filter.boost()),
                      String.format(
                              "{\"filter\":%s,\"scoreFunction\":%s",
                              filter, CSVWriter.builderToString(scoreFunction))
                          .replaceAll("\n", "")
                    };
                    writer.println(row);
                  } else {
                    throw new IllegalStateException(
                        "Unhandled score function: " + ffb.getScoreFunction());
                  }
                } else {
                  throw new IllegalStateException(
                      "Unhandled function score filter: " + ffb.getFilter());
                }
              }
            });
  }

  /**
   * Writes one row per clause of the relevancy query. The V2 query nests its clauses in bool
   * queries and the Search V3 Stage 1 query in dis_max queries, so the walk descends into both.
   */
  private static void writeRelevancyRows(
      CSVWriter writer, EntitySpec entitySpec, QueryBuilder query) {
    if (query instanceof BoolQueryBuilder) {
      BoolQueryBuilder bool = (BoolQueryBuilder) query;
      Stream.of(bool.must(), bool.should(), bool.filter())
          .flatMap(List::stream)
          .forEach(child -> writeRelevancyRows(writer, entitySpec, child));
    } else if (query instanceof DisMaxQueryBuilder) {
      ((DisMaxQueryBuilder) query)
          .innerQueries()
          .forEach(child -> writeRelevancyRows(writer, entitySpec, child));
    } else if (query instanceof SimpleQueryStringBuilder) {
      SimpleQueryStringBuilder sqsb = (SimpleQueryStringBuilder) query;
      for (Map.Entry<String, Float> fieldWeight : sqsb.fields().entrySet()) {
        String[] row = {
          entitySpec.getName(),
          "relevancy",
          "fulltext",
          sqsb.getClass().getSimpleName(),
          fieldWeight.getKey(),
          fieldWeight.getValue().toString(),
          // Unset when each field applies its own search analyzer
          Objects.toString(sqsb.analyzer(), ""),
          "true",
          String.valueOf(sqsb.boost()),
          sqsb.toString().replaceAll("\n", "")
        };
        writer.println(row);
      }
    } else if (query instanceof TermQueryBuilder) {
      writeTermRow(writer, entitySpec, (TermQueryBuilder) query, query);
    } else if (query instanceof ConstantScoreQueryBuilder
        && ((ConstantScoreQueryBuilder) query).innerQuery() instanceof TermQueryBuilder) {
      // An exact match scored with a constant instead of BM25
      writeTermRow(
          writer,
          entitySpec,
          (TermQueryBuilder) ((ConstantScoreQueryBuilder) query).innerQuery(),
          query);
    } else if (query instanceof MatchPhrasePrefixQueryBuilder) {
      MatchPhrasePrefixQueryBuilder mppqb = (MatchPhrasePrefixQueryBuilder) query;
      writeFieldRow(writer, entitySpec, "prefix_match", mppqb, mppqb.fieldName());
    } else if (query instanceof MatchPhraseQueryBuilder) {
      // Word gram subfields
      MatchPhraseQueryBuilder mpqb = (MatchPhraseQueryBuilder) query;
      writeFieldRow(writer, entitySpec, "phrase_match", mpqb, mpqb.fieldName());
    } else if (query instanceof MatchQueryBuilder) {
      MatchQueryBuilder mqb = (MatchQueryBuilder) query;
      writeFieldRow(writer, entitySpec, "match", mqb, mqb.fieldName());
    } else if (query instanceof WildcardQueryBuilder) {
      WildcardQueryBuilder wqb = (WildcardQueryBuilder) query;
      writeFieldRow(writer, entitySpec, "wildcard_match", wqb, wqb.fieldName());
    } else {
      throw new IllegalStateException(
          "Unhandled relevancy query builder: " + query.getClass().getName());
    }
  }

  private static void writeTermRow(
      CSVWriter writer, EntitySpec entitySpec, TermQueryBuilder tqb, QueryBuilder query) {
    String[] row = {
      entitySpec.getName(),
      "relevancy",
      "exact_match",
      query.getClass().getSimpleName(),
      tqb.fieldName(),
      String.valueOf(query.boost()),
      KEYWORD_ANALYZER,
      String.valueOf(tqb.caseInsensitive()),
      "",
      query.toString().replaceAll("\n", "")
    };
    writer.println(row);
  }

  private static void writeFieldRow(
      CSVWriter writer,
      EntitySpec entitySpec,
      String matchCategory,
      QueryBuilder query,
      String fieldName) {
    String[] row = {
      entitySpec.getName(),
      "relevancy",
      matchCategory,
      query.getClass().getSimpleName(),
      fieldName,
      String.valueOf(query.boost()),
      "",
      "true",
      "",
      query.toString().replaceAll("\n", "")
    };
    writer.println(row);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
    if (!"csv".equals(req.getParameter("format"))) {
      resp.setStatus(400);
      return;
    }

    WebApplicationContext ctx =
        WebApplicationContextUtils.getRequiredWebApplicationContext(req.getServletContext());

    try {
      resp.setContentType("text/csv");
      PrintWriter out = resp.getWriter();
      writeSearchCsv(ctx, out);
      out.flush();
      resp.setStatus(200);
    } catch (Exception e) {
      log.error("Error rendering csv", e);
      resp.setStatus(500);
    }
  }
}
