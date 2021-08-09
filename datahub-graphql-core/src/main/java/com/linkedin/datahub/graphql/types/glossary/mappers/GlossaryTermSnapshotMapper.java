package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermSnapshotMapper implements ModelMapper<GlossaryTermSnapshot, GlossaryTerm> {

    public static final GlossaryTermSnapshotMapper INSTANCE = new GlossaryTermSnapshotMapper();

    public static GlossaryTerm map(@Nonnull final GlossaryTermSnapshot glossaryTerm) {
        return INSTANCE.apply(glossaryTerm);
    }

    @Override
    public GlossaryTerm apply(@Nonnull final GlossaryTermSnapshot glossaryTerm) {
        GlossaryTerm result = new GlossaryTerm();
        result.setUrn(glossaryTerm.getUrn().toString());
        result.setType(EntityType.GLOSSARY_TERM);
        result.setName(GlossaryTermUtils.getGlossaryTermName(glossaryTerm.getUrn().getNameEntity()));
        result.setHierarchicalName(glossaryTerm.getUrn().getNameEntity());
        ModelUtils.getAspectsFromSnapshot(glossaryTerm).forEach(aspect -> {
            if (aspect instanceof GlossaryTermInfo) {
                result.setGlossaryTermInfo(GlossaryTermInfoMapper.map(GlossaryTermInfo.class.cast(aspect)));
            }
            if (aspect instanceof Ownership) {
                result.setOwnership(OwnershipMapper.map(Ownership.class.cast(aspect)));
            }
        });
        return result;
    }
}
