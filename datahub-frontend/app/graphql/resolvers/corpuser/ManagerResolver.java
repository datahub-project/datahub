package graphql.resolvers.corpuser;

import com.linkedin.common.Ownership;
import com.linkedin.data.template.RecordTemplate;
import graphql.resolvers.AuthenticatedResolver;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static graphql.Constants.*;

/**
 * Resolver responsible for resolving the 'manager' field of CorpUser.
 */
public class ManagerResolver extends AuthenticatedResolver<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> authenticatedGet(DataFetchingEnvironment environment) throws Exception {
        final CompletableFuture<Map<String, Object>> parent = environment.getSource();
        final String corpUserUrn = (String) parent.get().get(MANAGER_FIELD_NAME);
        if (corpUserUrn != null) {
            final DataLoader<String, Ownership> dataLoader = environment.getDataLoader(CORP_USER_LOADER_NAME);
            return dataLoader.load(corpUserUrn).thenApply(RecordTemplate::data);
        }
        return CompletableFuture.completedFuture(null);
    }
}
