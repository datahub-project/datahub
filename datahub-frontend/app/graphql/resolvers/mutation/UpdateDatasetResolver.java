package graphql.resolvers.mutation;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.dataset.Dataset;
import graphql.QueryContext;
import graphql.resolvers.AuthenticatedResolver;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import utils.GraphQLUtil;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Constants.*;

/**
 * Resolver responsible for updating a Dataset
 */
public class UpdateDatasetResolver extends AuthenticatedResolver<CompletableFuture<Map<String, Object>>> {

    @Override
    public CompletableFuture<Map<String, Object>> authenticatedGet(DataFetchingEnvironment environment) {

        /*
            Extract arguments
         */
        final Map<String, Object> input = environment.getArgument(INPUT_FIELD_NAME);
        final String datasetUrn = (String) input.get(URN_FIELD_NAME);

        /*
            Update Dataset using the input object. Type system assumed to be compatible with the public
            Dataset.pdl model.
         */
        try {
            Dataset dataset = new Dataset(GraphQLUtil.toDataMap(input));
            addAuditStamps(dataset, environment.getContext());
            DaoFactory.getDatasetsDao().updateDataset(datasetUrn, dataset);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to update dataset with URN %s", datasetUrn), e);
        }

        /*
            Fetch the latest version of the dataset.
         */
        final DataLoader<String, Dataset> dataLoader = environment.getDataLoader(DATASET_LOADER_NAME);
        return dataLoader.load(datasetUrn)
                .thenApply(RecordTemplate::data);
    }

    /**
     * Set fields required by Rest.li Validation
     *
     * TODO: This primarily serves to set required AuditStamps, even though they are replaced
     * at the GMA layer. Ideally audit stamps should appear as readOnly fields in the API
     * to avoid having to set them.
     */
    private void addAuditStamps(Dataset dataset, QueryContext context) {
        if (dataset.hasOwnership()) {
            Ownership ownership = dataset.getOwnership();
            // This is required for validation, but will be replaced at GMA.
            AuditStamp auditStamp = new AuditStamp();
            auditStamp.setActor(new CorpuserUrn(context.getUserName()));
            auditStamp.setTime(System.currentTimeMillis());
            ownership.setLastModified(auditStamp);
        }
    }
}