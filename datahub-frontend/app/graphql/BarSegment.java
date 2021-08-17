package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class BarSegment implements java.io.Serializable {

    private String label;
    private int value;

    public BarSegment() {
    }

    public BarSegment(String label, int value) {
        this.label = label;
        this.value = value;
    }

    public String getLabel() {
        return label;
    }
    public void setLabel(String label) {
        this.label = label;
    }

    public int getValue() {
        return value;
    }
    public void setValue(int value) {
        this.value = value;
    }



    public static BarSegment.Builder builder() {
        return new BarSegment.Builder();
    }

    public static class Builder {

        private String label;
        private int value;

        public Builder() {
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setValue(int value) {
            this.value = value;
            return this;
        }


        public BarSegment build() {
            return new BarSegment(label, value);
        }

    }
}
