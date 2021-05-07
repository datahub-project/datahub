package com.linkedin.metadata.resources.testentity;

import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.experimental.TestEntityAspect;
import com.linkedin.experimental.TestEntityKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.restli.BaseEntityResource;
import com.linkedin.metadata.snapshot.TestEntitySnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.tag.Tag;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;

@RestLiCollection(name = "test", namespace = "com.linkedin.testentity", keyName = "test")
public final class TestEntities extends BaseEntityResource<
        // @formatter:off
        ComplexResourceKey<TestEntityKey, EmptyRecord>,
        Tag,
        TestEntityUrn,
        TestEntitySnapshot,
        TestEntityAspect
        > {
    // @formatter:on

    @Inject
    @Named("testEntityDAO")
    private BaseLocalDAO<TestEntityAspect, TestEntityUrn> _localDAO;

    @Inject
    @Named("tagSearchDAO")
    private BaseSearchDAO _esSearchDAO;

    public TestEntities() {
        super(TestEntitySnapshot.class, TestEntityAspect.class);
    }


    @Nonnull
    @Override
    protected BaseLocalDAO<TestEntityAspect, TestEntityUrn> getLocalDAO() {
        return _localDAO;
    }

    @Nonnull
    @Override
    protected TestEntityUrn createUrnFromString(@Nonnull String urnString) throws Exception {
        return null;
    }

    @Nonnull
    @Override
    protected TestEntityUrn toUrn(@Nonnull ComplexResourceKey<TestEntityKey, EmptyRecord> tagKeyEmptyRecordComplexResourceKey) {
        return null;
    }

    @Nonnull
    @Override
    protected ComplexResourceKey<TestEntityKey, EmptyRecord> toKey(@Nonnull TestEntityUrn urn) {
        return null;
    }

    @Nonnull
    @Override
    protected Tag toValue(@Nonnull TestEntitySnapshot tagSnapshot) {
        return null;
    }

    @Nonnull
    @Override
    protected TestEntitySnapshot toSnapshot(@Nonnull Tag tag, @Nonnull TestEntityUrn urn) {
        return null;
    }

    @Action(name = ACTION_INGEST)
    @Override
    @Nonnull
    public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull TestEntitySnapshot snapshot) {
        return super.ingest(snapshot);
    }
}
