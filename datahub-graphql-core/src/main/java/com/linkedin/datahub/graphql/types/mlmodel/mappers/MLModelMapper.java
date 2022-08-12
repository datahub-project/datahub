package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Cost;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelEditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.CostMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLModelKey;
import com.linkedin.ml.metadata.CaveatsAndRecommendations;
import com.linkedin.ml.metadata.EditableMLModelProperties;
import com.linkedin.ml.metadata.EthicalConsiderations;
import com.linkedin.ml.metadata.EvaluationData;
import com.linkedin.ml.metadata.IntendedUse;
import com.linkedin.ml.metadata.MLModelFactorPrompts;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.ml.metadata.Metrics;
import com.linkedin.ml.metadata.QuantitativeAnalyses;
import com.linkedin.ml.metadata.SourceCode;
import com.linkedin.ml.metadata.TrainingData;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLModelMapper implements ModelMapper<EntityResponse, MLModel> {

    public static final MLModelMapper INSTANCE = new MLModelMapper();

    public static MLModel map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLModel apply(@Nonnull final EntityResponse entityResponse) {
        final MLModel result = new MLModel();
        Urn entityUrn = entityResponse.getUrn();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLMODEL);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        Long lastIngested = SystemMetadataUtils.getLastIngested(aspectMap);
        result.setLastIngested(lastIngested);

        MappingHelper<MLModel> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(ML_MODEL_KEY_ASPECT_NAME, this::mapMLModelKey);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
        mappingHelper.mapToResult(ML_MODEL_PROPERTIES_ASPECT_NAME, (entity, dataMap) -> this.mapMLModelProperties(entity, dataMap, entityUrn));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (mlModel, dataMap) -> this.mapGlobalTags(mlModel, dataMap, entityUrn));
        mappingHelper.mapToResult(INTENDED_USE_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setIntendedUse(IntendedUseMapper.map(new IntendedUse(dataMap))));
        mappingHelper.mapToResult(ML_MODEL_FACTOR_PROMPTS_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setFactorPrompts(MLModelFactorPromptsMapper.map(new MLModelFactorPrompts(dataMap))));
        mappingHelper.mapToResult(METRICS_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setMetrics(MetricsMapper.map(new Metrics(dataMap))));
        mappingHelper.mapToResult(EVALUATION_DATA_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setEvaluationData(new EvaluationData(dataMap).getEvaluationData()
                .stream().map(BaseDataMapper::map)
                .collect(Collectors.toList())));
        mappingHelper.mapToResult(TRAINING_DATA_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setTrainingData(new TrainingData(dataMap).getTrainingData()
                .stream().map(BaseDataMapper::map)
                .collect(Collectors.toList())));
        mappingHelper.mapToResult(QUANTITATIVE_ANALYSES_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setQuantitativeAnalyses(QuantitativeAnalysesMapper.map(new QuantitativeAnalyses(dataMap))));
        mappingHelper.mapToResult(ETHICAL_CONSIDERATIONS_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setEthicalConsiderations(EthicalConsiderationsMapper.map(new EthicalConsiderations(dataMap))));
        mappingHelper.mapToResult(CAVEATS_AND_RECOMMENDATIONS_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setCaveatsAndRecommendations(CaveatsAndRecommendationsMapper.map(new CaveatsAndRecommendations(dataMap))));
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(SOURCE_CODE_ASPECT_NAME, this::mapSourceCode);
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(COST_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setCost(CostMapper.map(new Cost(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlModel, dataMap) ->
            mlModel.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (entity, dataMap) ->
            entity.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
        mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapMLModelKey(MLModel mlModel, DataMap dataMap) {
        MLModelKey mlModelKey = new MLModelKey(dataMap);
        mlModel.setName(mlModelKey.getName());
        mlModel.setOrigin(FabricType.valueOf(mlModelKey.getOrigin().toString()));
        DataPlatform partialPlatform = new DataPlatform();
        partialPlatform.setUrn(mlModelKey.getPlatform().toString());
        mlModel.setPlatform(partialPlatform);
    }

    private void mapMLModelProperties(MLModel mlModel, DataMap dataMap, Urn entityUrn) {
        MLModelProperties modelProperties = new MLModelProperties(dataMap);
        mlModel.setProperties(MLModelPropertiesMapper.map(modelProperties, entityUrn));
        if (modelProperties.getDescription() != null) {
            mlModel.setDescription(modelProperties.getDescription());
        }
    }

    private void mapGlobalTags(MLModel mlModel, DataMap dataMap, Urn entityUrn) {
        GlobalTags globalTags = new GlobalTags(dataMap);
        com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags = GlobalTagsMapper.map(globalTags, entityUrn);
        mlModel.setGlobalTags(graphQlGlobalTags);
        mlModel.setTags(graphQlGlobalTags);
    }

    private void mapSourceCode(MLModel mlModel, DataMap dataMap) {
        SourceCode sourceCode = new SourceCode(dataMap);
        com.linkedin.datahub.graphql.generated.SourceCode graphQlSourceCode =
            new com.linkedin.datahub.graphql.generated.SourceCode();
        graphQlSourceCode.setSourceCode(sourceCode.getSourceCode().stream()
            .map(SourceCodeUrlMapper::map).collect(Collectors.toList()));
        mlModel.setSourceCode(graphQlSourceCode);
    }

    private void mapDomains(@Nonnull MLModel entity, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        // Currently we only take the first domain if it exists.
        entity.setDomain(DomainAssociationMapper.map(domains, entity.getUrn()));
    }

    private void mapEditableProperties(MLModel entity, DataMap dataMap) {
        EditableMLModelProperties input = new EditableMLModelProperties(dataMap);
        MLModelEditableProperties editableProperties = new MLModelEditableProperties();
        if (input.hasDescription()) {
            editableProperties.setDescription(input.getDescription());
        }
        entity.setEditableProperties(editableProperties);
    }
}
