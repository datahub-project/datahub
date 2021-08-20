package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class DateRange implements java.io.Serializable {

    private String start;
    private String end;

    public DateRange() {
    }

    public DateRange(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() {
        return start;
    }
    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }
    public void setEnd(String end) {
        this.end = end;
    }



    public static DateRange.Builder builder() {
        return new DateRange.Builder();
    }

    public static class Builder {

        private String start;
        private String end;

        public Builder() {
        }

        public Builder setStart(String start) {
            this.start = start;
            return this;
        }

        public Builder setEnd(String end) {
            this.end = end;
            return this;
        }


        public DateRange build() {
            return new DateRange(start, end);
        }

    }
}
