package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class NumericDataPoint implements java.io.Serializable {

    private String x;
    private int y;

    public NumericDataPoint() {
    }

    public NumericDataPoint(String x, int y) {
        this.x = x;
        this.y = y;
    }

    public String getX() {
        return x;
    }
    public void setX(String x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }
    public void setY(int y) {
        this.y = y;
    }



    public static NumericDataPoint.Builder builder() {
        return new NumericDataPoint.Builder();
    }

    public static class Builder {

        private String x;
        private int y;

        public Builder() {
        }

        public Builder setX(String x) {
            this.x = x;
            return this;
        }

        public Builder setY(int y) {
            this.y = y;
            return this;
        }


        public NumericDataPoint build() {
            return new NumericDataPoint(x, y);
        }

    }
}
