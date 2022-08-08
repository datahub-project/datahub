package com.linkedin.datahub.graphql.types.common.mappers;


import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.CustomPropertiesEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CustomPropertiesMapper {

    public static final CustomPropertiesMapper INSTANCE = new CustomPropertiesMapper();

    public static List<CustomPropertiesEntry> map(@Nonnull final Map<String, String> input, @Nonnull Urn urn) {
        return INSTANCE.apply(input, urn);
    }

    public List<CustomPropertiesEntry> apply(@Nonnull final Map<String, String> input, @Nonnull Urn urn) {
        List<CustomPropertiesEntry> results = new ArrayList<>();
        for (String key : input.keySet()) {
            final CustomPropertiesEntry entry = new CustomPropertiesEntry();
            entry.setKey(key);
            entry.setValue(input.get(key));
            entry.setAssociatedUrn(urn.toString());
            results.add(entry);
        }
        return results;
    }
}
