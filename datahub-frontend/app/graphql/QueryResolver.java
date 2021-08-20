package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public interface QueryResolver {

    boolean isAnalyticsEnabled() throws Exception;

    java.util.List<AnalyticsChartGroup> getAnalyticsCharts() throws Exception;

    java.util.List<Highlight> getHighlights() throws Exception;

}