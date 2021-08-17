package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class NamedBar implements java.io.Serializable {

    private String name;
    private java.util.List<BarSegment> segments;

    public NamedBar() {
    }

    public NamedBar(String name, java.util.List<BarSegment> segments) {
        this.name = name;
        this.segments = segments;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public java.util.List<BarSegment> getSegments() {
        return segments;
    }
    public void setSegments(java.util.List<BarSegment> segments) {
        this.segments = segments;
    }



    public static NamedBar.Builder builder() {
        return new NamedBar.Builder();
    }

    public static class Builder {

        private String name;
        private java.util.List<BarSegment> segments;

        public Builder() {
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setSegments(java.util.List<BarSegment> segments) {
            this.segments = segments;
            return this;
        }


        public NamedBar build() {
            return new NamedBar(name, segments);
        }

    }
}
