package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Cost;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
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
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.MLModelSnapshot;
import com.linkedin.ml.metadata.CaveatsAndRecommendations;
import com.linkedin.ml.metadata.EthicalConsiderations;
import com.linkedin.ml.metadata.EvaluationData;
import com.linkedin.ml.metadata.IntendedUse;
import com.linkedin.ml.metadata.MLModelFactorPrompts;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.ml.metadata.Metrics;
import com.linkedin.ml.metadata.QuantitativeAnalyses;
import com.linkedin.ml.metadata.TrainingData;
import com.linkedin.metadata.key.MLModelKey;

import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLModelSnapshotMapper implements ModelMapper<MLModelSnapshot, MLModel> {

    public static final MLModelSnapshotMapper INSTANCE = new MLModelSnapshotMapper();

    public static MLModel map(@Nonnull final MLModelSnapshot mlModel) {
        return INSTANCE.apply(mlModel);
    }

    @Override
    public MLModel apply(@Nonnull final MLModelSnapshot mlModel) {
        final MLModel result = new MLModel();
        result.setUrn(mlModel.getUrn().toString());
        result.setType(EntityType.MLMODEL);
        result.setName(mlModel.getUrn().getMlModelNameEntity());
        result.setOrigin(FabricType.valueOf(mlModel.getUrn().getOriginEntity().toString()));

        ModelUtils.getAspectsFromSnapshot(mlModel).forEach(aspect -> {
            if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof MLModelKey) {
                MLModelKey mlModelKey = MLModelKey.class.cast(aspect);
                result.setName(mlModelKey.getName());
                result.setOrigin(FabricType.valueOf(mlModelKey.getOrigin().toString()));
                DataPlatform partialPlatform = new DataPlatform();
                partialPlatform.setUrn(mlModelKey.getPlatform().toString());
                result.setPlatform(partialPlatform);
            } else if (aspect instanceof MLModelProperties) {
                MLModelProperties modelProperties = MLModelProperties.class.cast(aspect);
                result.setProperties(MLModelPropertiesMapper.map(modelProperties));
                if (modelProperties.getDescription() != null) {
                    result.setDescription(modelProperties.getDescription());
                }
            } else if (aspect instanceof GlobalTags) {
                result.setGlobalTags(GlobalTagsMapper.map((GlobalTags) aspect));
                result.setTags(GlobalTagsMapper.map((GlobalTags) aspect));
            } else if (aspect instanceof IntendedUse) {
                IntendedUse intendedUse = IntendedUse.class.cast(aspect);
                result.setIntendedUse(IntendedUseMapper.map(intendedUse));
            } else if (aspect instanceof MLModelFactorPrompts) {
                MLModelFactorPrompts mlModelFactorPrompts = MLModelFactorPrompts.class.cast(aspect);
                result.setFactorPrompts(MLModelFactorPromptsMapper.map(mlModelFactorPrompts));
            } else if (aspect instanceof Metrics) {
                Metrics metrics = Metrics.class.cast(aspect);
                result.setMetrics(MetricsMapper.map(metrics));
            } else if (aspect instanceof EvaluationData) {
                EvaluationData evaluationData = EvaluationData.class.cast(aspect);
                result.setEvaluationData(
                    evaluationData.getEvaluationData().stream().map(BaseDataMapper::map).collect(Collectors.toList()));
            } else if (aspect instanceof TrainingData) {
                TrainingData trainingData = TrainingData.class.cast(aspect);
                result.setTrainingData(trainingData.getTrainingData().stream().map(BaseDataMapper::map).collect(Collectors.toList()));
            } else if (aspect instanceof QuantitativeAnalyses) {
                QuantitativeAnalyses quantitativeAnalyses = QuantitativeAnalyses.class.cast(aspect);
                result.setQuantitativeAnalyses(QuantitativeAnalysesMapper.map(quantitativeAnalyses));
            } else if (aspect instanceof EthicalConsiderations) {
                EthicalConsiderations ethicalConsiderations = EthicalConsiderations.class.cast(aspect);
                result.setEthicalConsiderations(EthicalConsiderationsMapper.map(ethicalConsiderations));
            } else if (aspect instanceof CaveatsAndRecommendations) {
                CaveatsAndRecommendations caveatsAndRecommendations = CaveatsAndRecommendations.class.cast(aspect);
                result.setCaveatsAndRecommendations(CaveatsAndRecommendationsMapper.map(caveatsAndRecommendations));
            } else if (aspect instanceof InstitutionalMemory) {
                InstitutionalMemory institutionalMemory = InstitutionalMemory.class.cast(aspect);
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(institutionalMemory));
            } else if (aspect instanceof com.linkedin.ml.metadata.SourceCode) {
                com.linkedin.ml.metadata.SourceCode sourceCodeFromSnapshot = com.linkedin.ml.metadata.SourceCode.class.cast(aspect);
                SourceCode sourceCode = new SourceCode();
                sourceCode.setSourceCode(
                    sourceCodeFromSnapshot
                        .getSourceCode()
                        .stream()
                        .map(SourceCodeUrlMapper::map)
                        .collect(Collectors.toList())
                );
                result.setSourceCode(sourceCode);
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                result.setStatus(StatusMapper.map(status));
            } else if (aspect instanceof Cost) {
                Cost cost = Cost.class.cast(aspect);
                result.setCost(CostMapper.map(cost));
            } else if (aspect instanceof Deprecation) {
                Deprecation deprecation = Deprecation.class.cast(aspect);
                result.setDeprecation(DeprecationMapper.map(deprecation));
            }
        });

        return result;
    }
}
