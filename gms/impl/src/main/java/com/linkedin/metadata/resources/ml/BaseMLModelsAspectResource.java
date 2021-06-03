package com.linkedin.metadata.resources.ml;

import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.MLModelAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.BaseVersionedAspectResource;
import com.linkedin.ml.MLModelKey;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.RestLiCollection;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Deprecated! Use {@link EntityResource} instead.
 */
@Deprecated
public class BaseMLModelsAspectResource<ASPECT extends RecordTemplate>
    extends BaseVersionedAspectResource<MLModelUrn, MLModelAspect, ASPECT> {
    private static final String ML_MODEL_KEY = MLModels.class.getAnnotation(RestLiCollection.class).keyName();

    public BaseMLModelsAspectResource(Class<ASPECT> aspectClass) {
        super(MLModelAspect.class, aspectClass);
    }

    @Inject
    @Named("entityService")
    private EntityService _entityService;

    @Nonnull
    @Override
    protected BaseLocalDAO<MLModelAspect, MLModelUrn> getLocalDAO() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    protected MLModelUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
        MLModelKey key = keys.<ComplexResourceKey<MLModelKey, EmptyRecord>>get(ML_MODEL_KEY).getKey();
        return new MLModelUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }

    @Nonnull
    protected EntityService getEntityService() {
        return _entityService;
    }
}
