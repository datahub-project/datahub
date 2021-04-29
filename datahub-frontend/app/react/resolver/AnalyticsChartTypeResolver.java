package react.resolver;

import graphql.BarChart;
import graphql.TableChart;
import graphql.TimeSeriesChart;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class AnalyticsChartTypeResolver implements TypeResolver {
    @Override
    public GraphQLObjectType getType(TypeResolutionEnvironment env) {
        if (env.getObject() instanceof TimeSeriesChart) {
            return env.getSchema().getObjectType("TimeSeriesChart");
        } else if (env.getObject() instanceof BarChart) {
            return env.getSchema().getObjectType("BarChart");
        } else if (env.getObject() instanceof TableChart) {
            return env.getSchema().getObjectType("TableChart");
        } else {
            throw new RuntimeException("Unrecognized object type provided to AnalyticsChart resolver");
        }
    }
}
