package com.linkedin.datahub.graphql.types;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.types.dataset.DatasetUtils;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetMapper;
import com.linkedin.dataset.Dataset;
import com.linkedin.metadata.restli.BaseBrowsableClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TypeUtils {
    static <GQLT extends RecordTemplate, GMST, U extends Urn> List<GQLT> batchGet(
            final List<String> urns,
            final LoadableType<GQLT> client,
            final java.util.function.Function<String, U> getUrnLambda
    ) {
        AtomicInteger index = new AtomicInteger(0);

        final Collection<List<U>> entityUrnBatches = urns.stream()
                .map(getUrnLambda)
                .collect(Collectors.groupingBy(x -> index.getAndIncrement() / 50))
                .values();

        final List<GMST> results = new ArrayList<>();

        entityUrnBatches.forEach(entityUrns -> {
            try {
                final Map<DatasetUrn, Dataset> batchEntityMap = client.batchGet(datasetUrns
                        .stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet()));

                datasets.addAll(batchDatasetMap.values());
            } catch (Exception e) {
                throw new RuntimeException("Failed to batch load Datasets", e);
            }
        });

        return datasets.stream()
                .map(gmsDataset -> gmsDataset == null ? null : DatasetMapper.map(gmsDataset))
                .collect(Collectors.toList());
    }
}
