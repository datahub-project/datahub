package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class AnalyticsChartGroup implements java.io.Serializable {

    private String title;
    private java.util.List<AnalyticsChart> charts;

    public AnalyticsChartGroup() {
    }

    public AnalyticsChartGroup(String title, java.util.List<AnalyticsChart> charts) {
        this.title = title;
        this.charts = charts;
    }

    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    public java.util.List<AnalyticsChart> getCharts() {
        return charts;
    }
    public void setCharts(java.util.List<AnalyticsChart> charts) {
        this.charts = charts;
    }



    public static AnalyticsChartGroup.Builder builder() {
        return new AnalyticsChartGroup.Builder();
    }

    public static class Builder {

        private String title;
        private java.util.List<AnalyticsChart> charts;

        public Builder() {
        }

        public Builder setTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder setCharts(java.util.List<AnalyticsChart> charts) {
            this.charts = charts;
            return this;
        }


        public AnalyticsChartGroup build() {
            return new AnalyticsChartGroup(title, charts);
        }

    }
}
