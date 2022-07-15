package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class StringMapMapper implements ModelMapper<Map<String, String>, List<StringMapEntry>> {

    public static final StringMapMapper INSTANCE = new StringMapMapper();

    public static List<StringMapEntry> map(@Nonnull final Map<String, String> input) {
        return INSTANCE.apply(input);
    }

    @Override
    public List<StringMapEntry> apply(@Nonnull final Map<String, String> input) {
        List<StringMapEntry> results = new ArrayList<>();
        for (String key : input.keySet()) {
            final StringMapEntry entry = new StringMapEntry();
            entry.setKey(key);
            entry.setValue(input.get(key));
            results.add(entry);
        }
        return results;
    }
}
