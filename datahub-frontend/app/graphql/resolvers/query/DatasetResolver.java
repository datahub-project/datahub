package graphql.resolvers.query;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.Dataset;
import graphql.resolvers.AuthenticatedResolver;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Constants.*;


/**
 * Resolver responsible for resolving the 'dataset' field of Query
 */
public class DatasetResolver extends AuthenticatedResolver<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> authenticatedGet(DataFetchingEnvironment environment) {
        final DataLoader<String, Dataset> dataLoader = environment.getDataLoader(DATASET_LOADER_NAME);
        return dataLoader.load(environment.getArgument(URN_FIELD_NAME))
                .thenApply(RecordTemplate::data);
    }
}
