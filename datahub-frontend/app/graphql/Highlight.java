package graphql;


@javax.annotation.Generated(
    value = "com.kobylynskyi.graphql.codegen.GraphQLCodegen",
    date = "2021-08-12T10:01:57-0700"
)
public class Highlight implements java.io.Serializable {

    private int value;
    private String title;
    private String body;

    public Highlight() {
    }

    public Highlight(int value, String title, String body) {
        this.value = value;
        this.title = title;
        this.body = body;
    }

    public int getValue() {
        return value;
    }
    public void setValue(int value) {
        this.value = value;
    }

    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }
    public void setBody(String body) {
        this.body = body;
    }



    public static Highlight.Builder builder() {
        return new Highlight.Builder();
    }

    public static class Builder {

        private int value;
        private String title;
        private String body;

        public Builder() {
        }

        public Builder setValue(int value) {
            this.value = value;
            return this;
        }

        public Builder setTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder setBody(String body) {
            this.body = body;
            return this;
        }


        public Highlight build() {
            return new Highlight(value, title, body);
        }

    }
}
