package com.linkedin.metadata.builders.graph.relationship;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.TrainedOn;
import com.linkedin.ml.metadata.TrainingData;


import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE;

public class TrainedOnBuilderFromTrainingData extends BaseRelationshipBuilder<TrainingData> {

    public TrainedOnBuilderFromTrainingData() {
        super(TrainingData.class);
    }

    @Nonnull
    @Override
    public <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull URN urn, @Nonnull TrainingData trainingData) {
        final List<TrainedOn> trainingDataList = trainingData.getTrainingData()
            .stream()
            .map(baseData -> new TrainedOn().setSource(urn).setDestination(baseData.getDataset()))
            .collect(Collectors.toList());

        return Collections.singletonList(new GraphBuilder.RelationshipUpdates(trainingDataList, REMOVE_ALL_EDGES_FROM_SOURCE));
    }
}
