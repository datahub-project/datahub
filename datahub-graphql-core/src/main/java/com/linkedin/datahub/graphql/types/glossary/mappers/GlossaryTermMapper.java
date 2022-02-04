package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.key.GlossaryTermKey;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


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
        GlossaryTerm result = new GlossaryTerm();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.GLOSSARY_TERM);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<GlossaryTerm> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(GLOSSARY_TERM_KEY_ASPECT_NAME, this::mapGlossaryTermKey);
        mappingHelper.mapToResult(GLOSSARY_TERM_INFO_ASPECT_NAME, (glossaryTerm, dataMap) ->
            glossaryTerm.setGlossaryTermInfo(GlossaryTermInfoMapper.map(new GlossaryTermInfo(dataMap))));
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (glossaryTerm, dataMap) ->
            glossaryTerm.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        return mappingHelper.getResult();
    }

    private void mapGlossaryTermKey(@Nonnull GlossaryTerm glossaryTerm, @Nonnull DataMap dataMap) {
        GlossaryTermKey glossaryTermKey = new GlossaryTermKey(dataMap);
        glossaryTerm.setName(GlossaryTermUtils.getGlossaryTermName(glossaryTermKey.getName()));
        glossaryTerm.setHierarchicalName(glossaryTermKey.getName());
    }
}
