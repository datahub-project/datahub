package com.linkedin.metadata.resources.ml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import com.linkedin.common.Cost;
import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.MLModelUrn;

import com.linkedin.metadata.aspect.MLModelAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;

import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.MLModelDocument;
import com.linkedin.metadata.snapshot.MLModelSnapshot;
import com.linkedin.ml.MLModel;
import com.linkedin.ml.MLModelKey;

import com.linkedin.ml.metadata.CaveatsAndRecommendations;
import com.linkedin.ml.metadata.EthicalConsiderations;
import com.linkedin.ml.metadata.EvaluationData;
import com.linkedin.ml.metadata.IntendedUse;
import com.linkedin.ml.metadata.MLModelFactorPrompts;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.ml.metadata.Metrics;
import com.linkedin.ml.metadata.QuantitativeAnalyses;
import com.linkedin.ml.metadata.SourceCode;
import com.linkedin.ml.metadata.TrainingData;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import static com.linkedin.metadata.restli.RestliConstants.*;

@RestLiCollection(name = "mlModels", namespace = "com.linkedin.ml", keyName = "model")
public class MLModels extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<MLModelKey, EmptyRecord>,
    MLModel,
    MLModelUrn,
    MLModelSnapshot,
    MLModelAspect,
    MLModelDocument> {

    public MLModels() {
        super(MLModelSnapshot.class, MLModelAspect.class);
    }

    @Inject
    @Named("mlModelDAO")
    private BaseLocalDAO<MLModelAspect, MLModelUrn> _localDAO;

    @Inject
    @Named("mlModelSearchDAO")
    private BaseSearchDAO<MLModelDocument> _esSearchDAO;

    @Nonnull
    @Override
    protected BaseSearchDAO<MLModelDocument> getSearchDAO() {
        return _esSearchDAO;
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<MLModelAspect, MLModelUrn> getLocalDAO() {
        return _localDAO;
    }

    @Nonnull
    @Override
    protected MLModelUrn createUrnFromString(@Nonnull String urnString) throws Exception {
        return MLModelUrn.createFromString(urnString);
    }

    @Nonnull
    @Override
    protected MLModelUrn toUrn(@Nonnull ComplexResourceKey<MLModelKey, EmptyRecord> key) {
        return new MLModelUrn(key.getKey().getPlatform(), key.getKey().getName(), key.getKey().getOrigin());
    }

    @Nonnull
    @Override
    protected ComplexResourceKey<MLModelKey, EmptyRecord> toKey(@Nonnull MLModelUrn urn) {
        return new ComplexResourceKey<>(
            new MLModelKey()
                .setName(urn.getMlModelNameEntity())
                .setOrigin(urn.getOriginEntity())
                .setPlatform(urn.getPlatformEntity()),
            new EmptyRecord());
    }

    @Nonnull
    @Override
    protected MLModel toValue(@Nonnull MLModelSnapshot snapshot) {
        final MLModel value = new MLModel()
            .setPlatform(snapshot.getUrn().getPlatformEntity())
            .setName(snapshot.getUrn().getMlModelNameEntity())
            .setOrigin(snapshot.getUrn().getOriginEntity());

        ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
            if (aspect instanceof CaveatsAndRecommendations) {
                CaveatsAndRecommendations caveatsAndRecommendations = CaveatsAndRecommendations.class.cast(aspect);
                value.setCaveatsAndRecommendations(caveatsAndRecommendations);
            } else if (aspect instanceof Cost) {
                Cost cost = Cost.class.cast(aspect);
                value.setCost(cost);
            } else if (aspect instanceof Deprecation) {
                Deprecation deprecation = Deprecation.class.cast(aspect);
                value.setDeprecation(deprecation);
            } else if (aspect instanceof EthicalConsiderations) {
                EthicalConsiderations ethicalConsiderations = EthicalConsiderations.class.cast(aspect);
                value.setEthicalConsiderations(ethicalConsiderations);
            } else if (aspect instanceof EvaluationData) {
                EvaluationData evaluationData = EvaluationData.class.cast(aspect);
                value.setEvaluationData(evaluationData);
            } else if (aspect instanceof InstitutionalMemory) {
                InstitutionalMemory institutionalMemory = InstitutionalMemory.class.cast(aspect);
                value.setInstitutionalMemory(institutionalMemory);
            } else if (aspect instanceof IntendedUse) {
                IntendedUse intendedUse = IntendedUse.class.cast(aspect);
                value.setIntendedUse(intendedUse);
            } else if (aspect instanceof Metrics) {
                Metrics metrics = Metrics.class.cast(aspect);
                value.setMetrics(metrics);
            } else if (aspect instanceof MLModelFactorPrompts) {
                MLModelFactorPrompts mlModelFactorPrompts = MLModelFactorPrompts.class.cast(aspect);
                value.setMlModelFactorPrompts(mlModelFactorPrompts);
            } else if (aspect instanceof MLModelProperties) {
                MLModelProperties modelProperties = MLModelProperties.class.cast(aspect);
                value.setMlModelProperties(modelProperties);
            } else if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                value.setOwnership(ownership);
            } else if (aspect instanceof QuantitativeAnalyses) {
                QuantitativeAnalyses quantitativeAnalyses = QuantitativeAnalyses.class.cast(aspect);
                value.setQuantitativeAnalyses(quantitativeAnalyses);
            } else if (aspect instanceof SourceCode) {
                SourceCode sourceCode = SourceCode.class.cast(aspect);
                value.setSourceCode(sourceCode);
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                value.setStatus(status);
            } else if (aspect instanceof TrainingData) {
                TrainingData trainingData = TrainingData.class.cast(aspect);
                value.setTrainingData(trainingData);
            }
        });
        return value;
    }

    /**
     * MLModelFactors are not reversible to MLModelFactorPrompts so generated Snapshot will not have factors.
     */
    @Nonnull
    @Override
    protected MLModelSnapshot toSnapshot(@Nonnull MLModel mlModel, @Nonnull MLModelUrn urn) {
        final List<MLModelAspect> aspects = new ArrayList<>();
        if (mlModel.hasCaveatsAndRecommendations()) {
          aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getCaveatsAndRecommendations()));
        }
        if (mlModel.hasCost()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getCost()));
        }
        if (mlModel.hasDeprecation()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getDeprecation()));
        }
        if (mlModel.hasEthicalConsiderations()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getEthicalConsiderations()));
        }
        if (mlModel.hasEvaluationData()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getEvaluationData()));
        }
        if (mlModel.hasInstitutionalMemory()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getInstitutionalMemory()));
        }
        if (mlModel.hasIntendedUse()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getIntendedUse()));
        }
        if (mlModel.hasMetrics()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getMetrics()));
        }
        if (mlModel.hasMlModelProperties()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getMlModelProperties()));
        }
        if (mlModel.hasOwnership()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getOwnership()));
        }
        if (mlModel.hasQuantitativeAnalyses()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getQuantitativeAnalyses()));
        }
        if (mlModel.hasSourceCode()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getSourceCode()));
        }
        if (mlModel.hasStatus()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getStatus()));
        }
        if (mlModel.hasTrainingData()) {
            aspects.add(ModelUtils.newAspectUnion(MLModelAspect.class, mlModel.getTrainingData()));
        }
        return ModelUtils.newSnapshot(MLModelSnapshot.class, urn, aspects);
    }

    @RestMethod.Get
    @Override
    @Nonnull
    public Task<MLModel> get(@Nonnull ComplexResourceKey<MLModelKey, EmptyRecord> key,
        @QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
        return super.get(key, aspectNames);
    }

    @RestMethod.BatchGet
    @Override
    @Nonnull
    public Task<Map<ComplexResourceKey<MLModelKey, EmptyRecord>, MLModel>> batchGet(
        @Nonnull Set<ComplexResourceKey<MLModelKey, EmptyRecord>> keys,
        @QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
        return super.batchGet(keys, aspectNames);
    }

    @RestMethod.GetAll
    @Nonnull
    public Task<List<MLModel>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
        @QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames,
        @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
        @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
        return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
    }

    @Finder(FINDER_SEARCH)
    @Override
    @Nonnull
    public Task<CollectionResult<MLModel, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
        @QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames,
        @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
        @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
        @PagingContextParam @Nonnull PagingContext pagingContext) {
        return super.search(input, aspectNames, filter, sortCriterion, pagingContext);
    }

    @Action(name = ACTION_AUTOCOMPLETE)
    @Override
    @Nonnull
    public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
        @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
        @ActionParam(PARAM_LIMIT) int limit) {
        return super.autocomplete(query, field, filter, limit);
    }

    @Action(name = ACTION_INGEST)
    @Override
    @Nonnull
    public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull MLModelSnapshot snapshot) {
        return super.ingest(snapshot);
    }

    @Action(name = ACTION_GET_SNAPSHOT)
    @Override
    @Nonnull
    public Task<MLModelSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
        @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
        return super.getSnapshot(urnString, aspectNames);
    }

    @Action(name = ACTION_BACKFILL)
    @Override
    @Nonnull
    public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
        @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
        return super.backfill(urnString, aspectNames);
    }
}