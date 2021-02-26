package com.linkedin.datahub.graphql.types.mappers;

import javax.annotation.Nonnull;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.types.common.mappers.CostMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapEntryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.CaveatsAndRecommendationsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.EthicalConsiderationsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.EvaluationDataMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.IntendedUseMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MetricsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.ModelFactorsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelPropertiesMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.QuantitativeAnalysesMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.TrainingDataMapper;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MlModelMapper implements ModelMapper<com.linkedin.ml.MLModel, MLModel> {

    public static final MlModelMapper INSTANCE = new MlModelMapper();

    public static MLModel map(@Nonnull final com.linkedin.ml.MLModel mlModel) {
        return INSTANCE.apply(mlModel);
    }

    @Override
    public MLModel apply(@Nonnull final com.linkedin.ml.MLModel mlModel) {
        MLModel result = new MLModel();
        result.setUrn(mlModel.getUrn().toString());
        result.setType(EntityType.MLMODEL);
        result.setName(mlModel.getName());
        result.setDescription(mlModel.getDescription());
        result.setOrigin(Enum.valueOf(FabricType.class, mlModel.getOrigin().name()));
        result.setTags(mlModel.getTags());

        if (mlModel.getProperties() != null) {
            result.setProperties(StringMapMapper.map(mlModel.getProperties()));
        }
        if (mlModel.getOwnership() != null) {
            result.setOwnership(OwnershipMapper.map(mlModel.getOwnership()));
        }
        if (mlModel.getInstitutionalMemory() != null) {
            result.setInstitutionalMemory(InstitutionalMemoryMapper.map(mlModel.getInstitutionalMemory()));
        }
        if(mlModel.getMlModelProperties() != null) {
            result.setMlModelProperties(MLModelPropertiesMapper.map(mlModel.getMlModelProperties()));
        }
        if(mlModel.getIntendedUse() != null) {
            result.setIntendedUse(IntendedUseMapper.map(mlModel.getIntendedUse()));
        }
        if(mlModel.getMlModelFactorPrompts() != null) {
            result.setMlModelFactorPrompts(ModelFactorsMapper.map(mlModel.getMlModelFactorPrompts()));
        }
        if(mlModel.getMetrics() != null) {
            result.setMetrics(MetricsMapper.map(mlModel.getMetrics()));
        }
        if(mlModel.getEvaluationData() != null) {
            result.setEvaluationData(EvaluationDataMapper.map(mlModel.getEvaluationData()));
        }
        if(mlModel.getTrainingData() != null) {
            result.setTrainingData(TrainingDataMapper.map(mlModel.getTrainingData()));
        }
        if(mlModel.getCost() != null) {
            result.setCost(CostMapper.map(mlModel.getCost()));
        }
        if(mlModel.getQuantitativeAnalyses() != null) {
            result.setQuantitativeAnalyses(QuantitativeAnalysesMapper.map(mlModel.getQuantitativeAnalyses()));
        }
        if(mlModel.getEthicalConsiderations() != null) {
            result.setEthicalConsiderations(EthicalConsiderationsMapper.map(mlModel.getEthicalConsiderations()));
        }
        if(mlModel.getCaveatsAndRecommendations() != null) {
            result.setCaveatsAndRecommendations(CaveatsAndRecommendationsMapper.map(mlModel.getCaveatsAndRecommendations()));
        }
        if (mlModel.getStatus() != null) {
            result.setStatus(StatusMapper.map(mlModel.getStatus()));
        }
        return result;
    }
}
