package graphql.resolvers.type;

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

import java.util.Map;

public class SearchResultTypeResolver implements TypeResolver {
    @Override
    public GraphQLObjectType getType(TypeResolutionEnvironment env) {
        Map<String, Object> javaObject = env.getObject();
        String urn = (String) javaObject.get("urn");
        /*
            This is the reason we need strongly-typed objects to work with.
         */
        if (urn.startsWith("urn:li:dataset")) {
            return env.getSchema().getObjectType("Dataset");
        } else if (urn.startsWith("urn:li:corpuser")) {
            return env.getSchema().getObjectType("CorpUser");
        } else {
            throw new RuntimeException("Unrecognized object type provided to type resolver");
        }
    }
}
