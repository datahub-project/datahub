package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class TableChart implements java.io.Serializable, AnalyticsChart {

    private String title;
    private java.util.List<String> columns;
    private java.util.List<Row> rows;

    public TableChart() {
    }

    public TableChart(String title, java.util.List<String> columns, java.util.List<Row> rows) {
        this.title = title;
        this.columns = columns;
        this.rows = rows;
    }

    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    public java.util.List<String> getColumns() {
        return columns;
    }
    public void setColumns(java.util.List<String> columns) {
        this.columns = columns;
    }

    public java.util.List<Row> getRows() {
        return rows;
    }
    public void setRows(java.util.List<Row> rows) {
        this.rows = rows;
    }



    public static TableChart.Builder builder() {
        return new TableChart.Builder();
    }

    public static class Builder {

        private String title;
        private java.util.List<String> columns;
        private java.util.List<Row> rows;

        public Builder() {
        }

        public Builder setTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder setColumns(java.util.List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setRows(java.util.List<Row> rows) {
            this.rows = rows;
            return this;
        }


        public TableChart build() {
            return new TableChart(title, columns, rows);
        }

    }
}
