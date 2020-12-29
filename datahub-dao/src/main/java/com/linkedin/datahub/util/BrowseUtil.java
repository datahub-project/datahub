package com.linkedin.datahub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntityArray;
import com.linkedin.metadata.query.BrowseResultMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.linkedin.datahub.util.RestliUtil.*;

public class BrowseUtil {

    @Nonnull
    public static JsonNode getJsonFromBrowseResult(@Nonnull BrowseResult browseResult) throws IOException {
        final ObjectNode node = OM.createObjectNode();
        final BrowseResultEntityArray browseResultEntityArray = browseResult.getEntities();
        node.set("elements", collectionToArrayNode(browseResultEntityArray.subList(0, browseResultEntityArray.size())));
        node.put("start", browseResult.getFrom());
        node.put("count", browseResult.getPageSize());
        node.put("total", browseResult.getNumEntities());
        node.set("metadata", toJsonNode(browseResult.getMetadata()));
        return node;
    }

    @Nonnull
    public static Map<String, Object> toGraphQLBrowseResponse(@Nonnull BrowseResult browseResult) {

        final Map<String, Object> result = new HashMap<>();

        /*
         * Populate paging fields
         */
        result.put("start", browseResult.getFrom());
        result.put("count", browseResult.getPageSize());
        result.put("total", browseResult.getNumEntities());

        /*
         * Populate browse entities
         */
        result.put("entities", toGraphQLEntities(browseResult.getEntities()));

        /*
         * Populate browse metadata
         */
        result.put("metadata", toGraphQLBrowseMetadata(browseResult.getMetadata()));

        return result;
    }

    private static List<Map<String, Object>> toGraphQLEntities(BrowseResultEntityArray entities) {
        return entities
                .stream()
                .map(RecordTemplate::data)
                .collect(Collectors.toList());
    }

    private static Map<String, Object> toGraphQLBrowseMetadata(BrowseResultMetadata metadata) {
        return metadata.data();
    }

}
