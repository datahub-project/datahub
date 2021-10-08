package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpGroupSnapshotMapper implements ModelMapper<CorpGroupSnapshot, CorpGroup> {

    public static final CorpGroupSnapshotMapper INSTANCE = new CorpGroupSnapshotMapper();

    public static CorpGroup map(@Nonnull final CorpGroupSnapshot corpGroup) {
        return INSTANCE.apply(corpGroup);
    }

    @Override
    public CorpGroup apply(@Nonnull final CorpGroupSnapshot corpGroup) {
        final CorpGroup result = new CorpGroup();
        result.setUrn(corpGroup.getUrn().toString());
        result.setType(EntityType.CORP_GROUP);
        result.setName(corpGroup.getUrn().getGroupNameEntity());

        ModelUtils.getAspectsFromSnapshot(corpGroup).forEach(aspect -> {
            if (aspect instanceof CorpGroupInfo) {
                result.setProperties(CorpGroupPropertiesMapper.map(CorpGroupInfo.class.cast(aspect)));
                result.setInfo(CorpGroupInfoMapper.map(CorpGroupInfo.class.cast(aspect)));
            }
        });
        return result;
    }
}
