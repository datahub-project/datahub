package datahub.hive.consumer.service;

import com.google.gson.JsonObject;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Interface for services that transform Hive lineage data into Metadata Change Proposals (MCPs).
 */
public interface MCPTransformerService {

    /**
     * Transforms lineage message to MCPs for the given entity.
     *
     * @param entityJson The JSON object representing the entity
     * @return A list of DataTemplate<DataMap> representing Aspects of the entity
     * @throws URISyntaxException If a URI syntax error occurs
     */
    List<DataTemplate<DataMap>> transformToMCP(JsonObject entityJson) throws URISyntaxException;
}
