package com.datahub.gms.servlet;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.KEYWORD_ANALYZER;

import com.datahub.gms.util.CSVWriter;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.WeightBuilder;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

@Slf4j
public class ConfigSearchExport extends HttpServlet {

  private ConfigurationProvider getConfigProvider(WebApplicationContext ctx) {
    return (ConfigurationProvider) ctx.getBean("configurationProvider");
  }

  private EntityRegistry getEntityRegistry(WebApplicationContext ctx) {
    return (EntityRegistry) ctx.getBean("entityRegistry");
  }

  private void writeSearchCsv(WebApplicationContext ctx, PrintWriter pw) {
    SearchConfiguration searchConfiguration = getConfigProvider(ctx).getElasticSearch().getSearch();
    EntityRegistry entityRegistry = getEntityRegistry(ctx);

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

    SEARCHABLE_ENTITY_TYPES.stream()
        .map(
            entityType -> {
              try {
                EntitySpec entitySpec =
                    entityRegistry.getEntitySpec(EntityTypeMapper.getName(entityType));
                return Optional.of(entitySpec);
              } catch (IllegalArgumentException e) {
                log.warn("Failed to resolve entity `{}`", entityType.name());
                return Optional.<EntitySpec>empty();
              }
            })
        .filter(Optional::isPresent)
        .forEach(
            entitySpecOpt -> {
              EntitySpec entitySpec = entitySpecOpt.get();
              SearchRequest searchRequest =
                  SearchRequestHandler.getBuilder(entitySpec, searchConfiguration, null)
                      .getSearchRequest(
                          "*",
                          null,
                          null,
                          0,
                          0,
                          new SearchFlags()
                              .setFulltext(true)
                              .setSkipHighlighting(true)
                              .setSkipAggregates(true),
                          null);

              FunctionScoreQueryBuilder rankingQuery =
                  ((FunctionScoreQueryBuilder)
                      ((BoolQueryBuilder) searchRequest.source().query()).must().get(0));
              BoolQueryBuilder relevancyQuery = (BoolQueryBuilder) rankingQuery.query();
              BoolQueryBuilder simpleQueryString =
                  (BoolQueryBuilder) relevancyQuery.should().get(0);
              BoolQueryBuilder exactPrefixMatch = (BoolQueryBuilder) relevancyQuery.should().get(1);

              for (QueryBuilder simpBuilder : simpleQueryString.should()) {
                SimpleQueryStringBuilder sqsb = (SimpleQueryStringBuilder) simpBuilder;
                for (Map.Entry<String, Float> fieldWeight : sqsb.fields().entrySet()) {
                  String[] row = {
                    entitySpec.getName(),
                    "relevancy",
                    "fulltext",
                    sqsb.getClass().getSimpleName(),
                    fieldWeight.getKey(),
                    fieldWeight.getValue().toString(),
                    sqsb.analyzer(),
                    "true",
                    String.valueOf(sqsb.boost()),
                    sqsb.toString().replaceAll("\n", "")
                  };
                  writer.println(row);
                }
              }

              for (QueryBuilder builder : exactPrefixMatch.should()) {
                if (builder instanceof TermQueryBuilder) {
                  TermQueryBuilder tqb = (TermQueryBuilder) builder;
                  String[] row = {
                    entitySpec.getName(),
                    "relevancy",
                    "exact_match",
                    tqb.getClass().getSimpleName(),
                    tqb.fieldName(),
                    String.valueOf(tqb.boost()),
                    KEYWORD_ANALYZER,
                    String.valueOf(tqb.caseInsensitive()),
                    "",
                    tqb.toString().replaceAll("\n", "")
                  };
                  writer.println(row);
                } else if (builder instanceof MatchPhrasePrefixQueryBuilder) {
                  MatchPhrasePrefixQueryBuilder mppqb = (MatchPhrasePrefixQueryBuilder) builder;
                  String[] row = {
                    entitySpec.getName(),
                    "relevancy",
                    "prefix_match",
                    mppqb.getClass().getSimpleName(),
                    mppqb.fieldName(),
                    String.valueOf(mppqb.boost()),
                    "",
                    "true",
                    "",
                    mppqb.toString().replaceAll("\n", "")
                  };
                  writer.println(row);
                } else {
                  throw new IllegalStateException(
                      "Unhandled exact prefix builder: " + builder.getClass().getName());
                }
              }

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
