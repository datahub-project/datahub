package com.linkedin.datahub.graphql.loaders;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.client.Datasets;
import org.dataloader.BatchLoader;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Responsible for fetching {@link Dataset} objects from the downstream GMS, leveraging
 * the public clients.
 */
public class DatasetLoader implements BatchLoader<String, Dataset> {

    public static final String NAME = "datasetLoader";

    private final Datasets _datasetsClient;

    public DatasetLoader(final Datasets datasetsClient) {
        _datasetsClient = datasetsClient;
    }

    @Override
    public CompletionStage<List<Dataset>> load(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<DatasetUrn> datasetUrns = keys.stream()
                        .map(this::getDatasetUrn)
                        .collect(Collectors.toList());

                Map<DatasetUrn, Dataset> datasetMap = _datasetsClient.batchGet(datasetUrns
                        .stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet()));

                List<Dataset> results = new ArrayList<>();
                for (DatasetUrn urn : datasetUrns) {
                    results.add(datasetMap.getOrDefault(urn, null));
                }
                return results;
            } catch (Exception e) {
                throw new RuntimeException("Failed to batch load Datasets", e);
            }
        });
    }

    private DatasetUrn getDatasetUrn(String urnStr) {
        try {
            return DatasetUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve dataset with urn %s, invalid urn", urnStr));
        }
    }
}
