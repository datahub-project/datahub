package com.checkout.lineage;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.metadata.key.DatasetKey;
import datahub.event.MetadataChangeProposalWrapper;
import lombok.SneakyThrows;
import org.apache.flink.api.dag.Transformation;

import java.util.ArrayList;
import java.util.List;

public class LineageParser {
    private final ArrayList<Transformation<?>> transformations;

    private final DatasetUrnArray upstreamDatasetsOverride;
    private final DatasetUrnArray upstreamDatasetsAppend;
    private final DatasetUrnArray downstreamDatasetsOverride;
    private final DatasetUrnArray downstreamDatasetsAppend;

    public LineageParser(ArrayList<Transformation<?>> transformations,
                         DatasetUrnArray upstreamDatasetsOverride,
                         DatasetUrnArray upstreamDatasetsAppend,
                         DatasetUrnArray downstreamDatasetsOverride,
                         DatasetUrnArray downstreamDatasetsAppend) {
        this.transformations = transformations;
        this.upstreamDatasetsOverride = upstreamDatasetsOverride;
        this.upstreamDatasetsAppend = upstreamDatasetsAppend;
        this.downstreamDatasetsOverride = downstreamDatasetsOverride;
        this.downstreamDatasetsAppend = downstreamDatasetsAppend;

    }

    @SneakyThrows
    private DatasetUrnArray getSources() {
        if (upstreamDatasetsOverride != null) {
            return upstreamDatasetsOverride;
        }
        DatasetUrnArray datasetUrns = parseSources();

        if (upstreamDatasetsAppend != null) {
            datasetUrns.addAll(upstreamDatasetsAppend);
        }
        return datasetUrns;
    }

    private DatasetUrnArray parseSources() {
        return new DatasetUrnArray();
    }

    private DatasetUrnArray getSinks() {
        if (downstreamDatasetsOverride != null) {
            return downstreamDatasetsOverride;
        }
        DatasetUrnArray datasetUrns = parseSinks();

        if (downstreamDatasetsAppend != null) {
            datasetUrns.addAll(downstreamDatasetsAppend);
        }
        return datasetUrns;
    }

    private DatasetUrnArray parseSinks() {
        return new DatasetUrnArray();
    }

    public List<MetadataChangeProposalWrapper<?>> getProposals(Urn jobUrn) {
        DatasetUrnArray sourceUrns = getSources();
        DatasetUrnArray sinkUrns = getSinks();

        ArrayList<MetadataChangeProposalWrapper<?>> proposals = new ArrayList<>();

        List<DatasetUrn> allUrns = new ArrayList<>(sourceUrns);
        allUrns.addAll(sinkUrns);

        for (DatasetUrn sourceUrn : allUrns) {
            proposals.add(MetadataChangeProposalWrapper.builder()
                    .entityType("dataset")
                    .entityUrn(sourceUrn)
                    .upsert()
                    .aspect(new DatasetKey().setName(sourceUrn.getDatasetNameEntity())
                            .setOrigin(sourceUrn.getOriginEntity())
                            .setPlatform(sourceUrn.getPlatformEntity())).build());
        }

        proposals.add(MetadataChangeProposalWrapper.builder()
                .entityType("dataJob")
                .entityUrn(jobUrn)
                .upsert()
                .aspect(new DataJobInputOutput()
                        .setInputDatasets(sourceUrns)
                        .setOutputDatasets(sinkUrns))
                .build());

        return proposals;
    }


}
