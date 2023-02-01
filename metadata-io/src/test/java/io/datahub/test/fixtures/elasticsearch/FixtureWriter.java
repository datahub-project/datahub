package io.datahub.test.fixtures.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Builder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.function.BiConsumer;

import static io.datahub.test.fixtures.elasticsearch.Utils.OBJECT_MAPPER;

/**
 *
 */
@Builder
public class FixtureWriter {

    private RestHighLevelClient client;

    @Builder.Default
    private String outputBase = Utils.FIXTURE_BASE;

    public void write(SearchRequest searchRequest, String relativeOutput, boolean append) {
        write(searchRequest, relativeOutput, append, null, null, null);
    }

    public <O, C> void write(SearchRequest searchRequest, String relativeOutput, boolean append,
                             @Nullable Class<O> outputType, Class<C> callbackType, BiConsumer<SearchHit, C> callback) {
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            long remainingHits = hits.getTotalHits().value;

            if (remainingHits > 0) {
                try (FileWriter writer = new FileWriter(String.format("%s/%s", outputBase, relativeOutput), append);
                     BufferedWriter bw = new BufferedWriter(writer)) {

                    while (remainingHits > 0) {
                        SearchHit lastHit = null;
                        for (SearchHit hit : hits.getHits()) {
                            lastHit = hit;
                            remainingHits -= 1;

                            try {
                                if (outputType == null) {
                                    bw.write(hit.getSourceAsString());
                                } else {
                                    O doc = OBJECT_MAPPER.readValue(hit.getSourceAsString(), outputType);
                                    bw.write(OBJECT_MAPPER.writeValueAsString(doc));
                                }
                                bw.newLine();

                                // Fire callback
                                if (callback != null) {
                                    callback.accept(hit, OBJECT_MAPPER.readValue(hit.getSourceAsString(), callbackType));
                                }
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        if (lastHit != null) {
                            searchRequest.source().searchAfter(lastHit.getSortValues());
                            hits = client.search(searchRequest, RequestOptions.DEFAULT).getHits();
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
