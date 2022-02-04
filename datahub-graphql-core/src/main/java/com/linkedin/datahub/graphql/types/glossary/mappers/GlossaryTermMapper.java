package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.entity.EntityResponse;
import static com.linkedin.metadata.Constants.*;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermMapper implements ModelMapper<EntityResponse, GlossaryTerm> {

    public static final GlossaryTermMapper INSTANCE = new GlossaryTermMapper();

    public static GlossaryTerm map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public GlossaryTerm apply(@Nonnull final EntityResponse entityResponse) {
        final GlossaryTerm result = new GlossaryTerm();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.GLOSSARY_TERM);
        entityResponse.getAspects().forEach((name, aspect) -> {
            DataMap data = aspect.getValue().data();
            if (GLOSSARY_TERM_KEY_ASPECT_NAME.equals(name)) {
                final GlossaryTermKey gmsKey = new GlossaryTermKey(data);
                result.setName(GlossaryTermUtils.getGlossaryTermName(gmsKey.getName()));
                result.setHierarchicalName(gmsKey.getName());
            } else if (GLOSSARY_TERM_INFO_ASPECT_NAME.equals(name)) {
                result.setGlossaryTermInfo(GlossaryTermInfoMapper.map(new GlossaryTermInfo(data)));
            } else if (OWNERSHIP_ASPECT_NAME.equals(name)) {
                result.setOwnership(OwnershipMapper.map(new Ownership(data)));
            }
        });
        return result;
    }
}
