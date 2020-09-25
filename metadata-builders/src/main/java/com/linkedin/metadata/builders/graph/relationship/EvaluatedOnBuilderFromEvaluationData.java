package com.linkedin.metadata.builders.graph.relationship;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.EvaluatedOn;
import com.linkedin.ml.metadata.BaseData;
import com.linkedin.ml.metadata.EvaluationData;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE;

public class EvaluatedOnBuilderFromEvaluationData extends BaseRelationshipBuilder<EvaluationData> {

    public EvaluatedOnBuilderFromEvaluationData() {
        super(EvaluationData.class);
    }

    @Nonnull
    @Override
    public <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull URN urn, @Nonnull EvaluationData evaluationData) {
        final List<EvaluatedOn> evaluationDataList = evaluationData.getEvaluationData()
            .stream()
            .filter(BaseData::hasDataset)
            .filter(baseData -> DatasetUrn.ENTITY_TYPE.equals(baseData.getDataset().getEntityType()))
            .map(baseData -> {
                EvaluatedOn evaluatedOn = new EvaluatedOn().setSource(urn).setDestination(baseData.getDataset());
                if (baseData.hasPreProcessing()) {
                    evaluatedOn.setPreProcessing(baseData.getPreProcessing().toString());
                }
                return evaluatedOn;
            })
            .collect(Collectors.toList());

        return Collections.singletonList(new GraphBuilder.RelationshipUpdates(evaluationDataList, REMOVE_ALL_EDGES_FROM_SOURCE));
    }
}
