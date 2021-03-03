package com.linkedin.datahub.graphql.types.tag;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.types.mappers.TagMapper;
import com.linkedin.tag.client.Tags;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TagType implements com.linkedin.datahub.graphql.types.EntityType<Tag> {

    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "name";

    private final Tags _tagClient;

    public TagType(final Tags tagClient) {
        _tagClient = tagClient;
    }

    @Override
    public Class<Tag> objectClass() {
        return Tag.class;
    }

    @Override
    public EntityType type() {
        return EntityType.TAG;
    }

    @Override
    public List<Tag> batchLoad(final List<String> urns, final QueryContext context) {

        final List<TagUrn> tagUrns = urns.stream()
                .map(this::getTagUrn)
                .collect(Collectors.toList());

        try {
            final Map<TagUrn, com.linkedin.tag.Tag> tagMap = _tagClient.batchGet(tagUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<com.linkedin.tag.Tag> gmsResults = new ArrayList<>();
            for (TagUrn urn : tagUrns) {
                gmsResults.add(tagMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsTag -> gmsTag == null ? null : TagMapper.map(gmsTag))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Tags", e);
        }
    }

    private TagUrn getTagUrn(final String urnStr) {
        try {
            return TagUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve tag with urn %s, invalid urn", urnStr));
        }
    }
}
