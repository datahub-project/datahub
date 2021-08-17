package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class Row implements java.io.Serializable {

    private java.util.List<String> values;

    public Row() {
    }

    public Row(java.util.List<String> values) {
        this.values = values;
    }

    /**
     * All values are expected to be strings.
     */
    public java.util.List<String> getValues() {
        return values;
    }
    /**
     * All values are expected to be strings.
     */
    public void setValues(java.util.List<String> values) {
        this.values = values;
    }



    public static Row.Builder builder() {
        return new Row.Builder();
    }

    public static class Builder {

        private java.util.List<String> values;

        public Builder() {
        }

        /**
         * All values are expected to be strings.
         */
        public Builder setValues(java.util.List<String> values) {
            this.values = values;
            return this;
        }


        public Row build() {
            return new Row(values);
        }

    }
}
