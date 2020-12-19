package graphql.resolvers.ownership;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUser;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Constants.CORP_USER_LOADER_NAME;
import static graphql.Constants.OWNER_FIELD_NAME;

/**
 * Resolver responsible for resolving the 'owner' field of Ownership.
 */
public class OwnerResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        final Map<String, Object> parent = environment.getSource();
        final DataLoader<String, CorpUser> dataLoader = environment.getDataLoader(CORP_USER_LOADER_NAME);
        return dataLoader.load((String) parent.get(OWNER_FIELD_NAME))
                .thenApply(RecordTemplate::data);
    }
}