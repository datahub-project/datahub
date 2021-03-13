package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.SourceCode;
import com.linkedin.datahub.graphql.types.common.mappers.CostMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLModelMapper implements ModelMapper<com.linkedin.ml.MLModel, MLModel> {

    public static final MLModelMapper INSTANCE = new MLModelMapper();

    public static MLModel map(@Nonnull final com.linkedin.ml.MLModel mlModel) {
        return INSTANCE.apply(mlModel);
    }

    @Override
    public MLModel apply(@Nonnull final com.linkedin.ml.MLModel mlModel) {
        final MLModel result = new MLModel();
        result.setUrn(mlModel.getUrn().toString());
        result.setType(EntityType.MLMODEL);
        result.setName(mlModel.getName());
        result.setDescription(mlModel.getDescription());
        result.setOrigin(FabricType.valueOf(mlModel.getOrigin().name()));
        result.setTags(mlModel.getTags());

        if (mlModel.getOwnership() != null) {
            result.setOwnership(OwnershipMapper.map(mlModel.getOwnership()));
        }
        if (mlModel.getMlModelProperties() != null) {
            result.setProperties(MLModelPropertiesMapper.map(mlModel.getMlModelProperties()));
        }
        if (mlModel.getIntendedUse() != null) {
            result.setIntendedUse(IntendedUseMapper.map(mlModel.getIntendedUse()));
        }
        if (mlModel.getMlModelFactorPrompts() != null) {
            result.setFactorPrompts(MLModelFactorPromptsMapper.map(mlModel.getMlModelFactorPrompts()));
        }
        if (mlModel.getMetrics() != null) {
            result.setMetrics(MetricsMapper.map(mlModel.getMetrics()));
        }
        if (mlModel.getEvaluationData() != null) {
            result.setEvaluationData(mlModel.getEvaluationData().getEvaluationData().stream().map(BaseDataMapper::map).collect(Collectors.toList()));
        }
        if (mlModel.getTrainingData() != null) {
            result.setTrainingData(mlModel.getTrainingData().getTrainingData().stream().map(BaseDataMapper::map).collect(Collectors.toList()));
        }
        if (mlModel.getQuantitativeAnalyses() != null) {
            result.setQuantitativeAnalyses(QuantitativeAnalysesMapper.map(mlModel.getQuantitativeAnalyses()));
        }
        if (mlModel.getEthicalConsiderations() != null) {
            result.setEthicalConsiderations(EthicalConsiderationsMapper.map(mlModel.getEthicalConsiderations()));
        }
        if (mlModel.getCaveatsAndRecommendations() != null) {
            result.setCaveatsAndRecommendations(CaveatsAndRecommendationsMapper.map(mlModel.getCaveatsAndRecommendations()));
        }
        if (mlModel.getInstitutionalMemory() != null) {
            result.setInstitutionalMemory(InstitutionalMemoryMapper.map(mlModel.getInstitutionalMemory()));
        }
        if (mlModel.getSourceCode() != null) {
            SourceCode sourceCode = new SourceCode();
            sourceCode.setSourceCode(
                mlModel.getSourceCode()
                    .getSourceCode()
                    .stream()
                    .map(SourceCodeUrlMapper::map)
                    .collect(Collectors.toList())
            );
            result.setSourceCode(sourceCode);
        }
        if (mlModel.getStatus() != null) {
            result.setStatus(StatusMapper.map(mlModel.getStatus()));
        }
        if (mlModel.getCost() != null) {
            result.setCost(CostMapper.map(mlModel.getCost()));
        }
        if (mlModel.getDeprecation() != null) {
            result.setDeprecation(DeprecationMapper.map(mlModel.getDeprecation()));
        }
        return result;
    }
}
