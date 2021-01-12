package com.linkedin.datahub.dao.table;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.dataset.client.Ownerships;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.linkedin.datahub.util.DatasetUtil.toDatasetUrn;
import static com.linkedin.datahub.util.DatasetUtil.toSnapshot;

/**
 * Data access object for Datasets and related aspects.
 */
public class DatasetsDao {

    private final Datasets _datasets;
    private final Ownerships _ownership;

    public DatasetsDao(@Nonnull Datasets datasets, @Nonnull Ownerships ownership) {
        _datasets = datasets;
        _ownership = ownership;
    }

    public List<String> getDatasetOwnerTypes() {
        return Arrays.asList("DataOwner", "Producer", "Delegate", "Stakeholder", "Consumer", "Developer");
    }

    @Nonnull
    public Dataset getDataset(@Nonnull String datasetUrn) throws Exception {
        return _datasets.get(toDatasetUrn(datasetUrn));
    }


    @Nonnull
    public List<Dataset> getDatasets(@Nonnull List<String> datasetUrnStrs) throws Exception {
        List<DatasetUrn> datasetUrns = datasetUrnStrs.stream().map(urnStr -> {
                try {
                    return toDatasetUrn(urnStr);
                } catch (URISyntaxException e) {
                    return null;
                }
            }).collect(Collectors.toList());

        Map<DatasetUrn, Dataset> datasetMap = _datasets.batchGet(datasetUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        List<Dataset> results = new ArrayList<>();
        for (DatasetUrn urn : datasetUrns) {
            results.add(datasetMap.getOrDefault(urn, null));
        }
        return results;
    }

    @Nonnull
    public Ownership getOwnership(@Nonnull String datasetUrn) throws Exception {
        return _ownership.getLatestOwnership(toDatasetUrn(datasetUrn));
    }

    @Nonnull
    public void updateDataset(@Nonnull String datasetUrnStr, @Nonnull Dataset partialDataset) throws Exception {
        DatasetUrn datasetUrn = toDatasetUrn(datasetUrnStr);
        _datasets.createSnapshot(datasetUrn, toSnapshot(datasetUrn, partialDataset));
    }
}