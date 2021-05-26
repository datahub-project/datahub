package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.common.urn.CorpuserUrn;

import java.net.URISyntaxException;
import javax.annotation.Nonnull;

public class InstitutionalMemoryMetadataMapper implements ModelMapper<com.linkedin.common.InstitutionalMemoryMetadata, InstitutionalMemoryMetadata> {

    public static final InstitutionalMemoryMetadataMapper INSTANCE = new InstitutionalMemoryMetadataMapper();

    public static InstitutionalMemoryMetadata map(@Nonnull final com.linkedin.common.InstitutionalMemoryMetadata metadata) {
        return INSTANCE.apply(metadata);
    }

    @Override
    public InstitutionalMemoryMetadata apply(@Nonnull final com.linkedin.common.InstitutionalMemoryMetadata input) {
        final InstitutionalMemoryMetadata result = new InstitutionalMemoryMetadata();
        result.setUrl(input.getUrl().toString());
        result.setDescription(input.getDescription());
        result.setAuthor(getAuthor(input.getCreateStamp().getActor().toString()));
        result.setCreated(AuditStampMapper.map(input.getCreateStamp()));
        return result;
    }

    private CorpUser getAuthor(String actor) {
        CorpUser corpUser = new CorpUser();
        try {
            CorpuserUrn corpuserUrn = CorpuserUrn.createFromString(actor);
            corpUser.setUrn(actor);
            corpUser.setUsername(corpuserUrn.getUsernameEntity());
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve author with urn %s, invalid urn", actor));
        }
        return corpUser;
    }
}
