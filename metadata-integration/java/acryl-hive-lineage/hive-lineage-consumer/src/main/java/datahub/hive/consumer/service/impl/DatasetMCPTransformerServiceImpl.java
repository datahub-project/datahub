package datahub.hive.consumer.service.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;

import datahub.hive.consumer.config.Constants;
import datahub.hive.consumer.service.MCPTransformerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MCPTransformerService for Dataset entities.
 */
@Service
@Slf4j
public class DatasetMCPTransformerServiceImpl implements MCPTransformerService {

    @Value("${application.environment}")
    private String environment;

    @Value("${application.service.user}")
    private String serviceUser;

    @Override
    public List<DataTemplate<DataMap>> transformToMCP(JsonObject datasetJsonObject) throws URISyntaxException {
        List<DataTemplate<DataMap>> aspects = new ArrayList<>();

        // Build upstreamLineage aspect
        aspects.add(buildUpstreamLineageAspect(datasetJsonObject));

        // Build dataPlatformInstance aspect
        aspects.add(buildDataPlatformInstanceAspect(datasetJsonObject));

        // Build status aspect
        aspects.add(buildStatusAspect());

        // Build subTypes aspect
        aspects.add(buildSubTypesAspect());

        return aspects;
    }

    /**
     * Build the upstream lineage aspect for the dataset.
     */
    private DataTemplate<DataMap> buildUpstreamLineageAspect(JsonObject datasetJsonObject) throws URISyntaxException {
        UpstreamLineage upstreamLineage = new UpstreamLineage();
        List<Upstream> upstreams = new ArrayList<>();
        
        // Process inputs as upstream datasets
        if (datasetJsonObject.has(Constants.INPUTS_KEY) && !datasetJsonObject.getAsJsonArray(Constants.INPUTS_KEY).isEmpty()) {
            for (var inputElement : datasetJsonObject.getAsJsonArray(Constants.INPUTS_KEY)) {
                String inputDataset = datasetJsonObject.get(Constants.PLATFORM_INSTANCE_KEY).getAsString() + "." + inputElement.getAsString();

                Upstream upstream = new Upstream();
                
                DatasetUrn upstreamUrn = new DatasetUrn(
                    new DataPlatformUrn(Constants.PLATFORM_NAME),
                    inputDataset,
                    FabricType.valueOf(environment)
                );
                upstream.setDataset(upstreamUrn);
                
                upstream.setType(DatasetLineageType.TRANSFORMED);
                
                if (datasetJsonObject.has(Constants.HASH_KEY)) {
                    String queryId = datasetJsonObject.get(Constants.HASH_KEY).getAsString();
                    Urn queryUrn = Urn.createFromString(Constants.QUERY_URN_PREFIX + queryId);
                    upstream.setQuery(queryUrn);
                }

                long timestamp = System.currentTimeMillis();
                
                AuditStamp created = new AuditStamp();
                created.setTime(timestamp);
                created.setActor(Urn.createFromString(Constants.CORP_USER_URN_PREFIX + serviceUser));
                upstream.setCreated(created);
                
                AuditStamp lastModified = new AuditStamp();
                lastModified.setTime(timestamp);
                lastModified.setActor(Urn.createFromString(Constants.CORP_USER_URN_PREFIX + serviceUser));
                upstream.setAuditStamp(lastModified);
                
                upstreams.add(upstream);
            }
        }
        
        List<FineGrainedLineage> fineGrainedLineages = new ArrayList<>();
        if (datasetJsonObject.has(Constants.EDGES_KEY) && datasetJsonObject.has(Constants.VERTICES_KEY)) {
            // Process edges to find column-level lineage
            for (var edgeElement : datasetJsonObject.getAsJsonArray(Constants.EDGES_KEY)) {
                JsonObject edge = edgeElement.getAsJsonObject();
                String edgeType = edge.get(Constants.EDGE_TYPE_KEY).getAsString();

                if (Constants.PROJECTION_KEY.equals(edgeType)) {
                    JsonArray sources = edge.getAsJsonArray(Constants.SOURCES_KEY);
                    JsonArray targets = edge.getAsJsonArray(Constants.TARGETS_KEY);
                    
                    if (sources != null && targets != null && !sources.isEmpty() && !targets.isEmpty()) {
                        FineGrainedLineage fgl = new FineGrainedLineage();
                        
                        fgl.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
                        fgl.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
                        
                        fgl.setConfidenceScore(1.0f);

                        // Map vertex IDs to field URNs
                        JsonArray vertices = datasetJsonObject.getAsJsonArray(Constants.VERTICES_KEY);
                        
                        // Process source vertices for upstream fields
                        List<String> upstreamFields = new ArrayList<>(processVertices(datasetJsonObject, vertices, sources, Constants.INPUTS_KEY));
                        
                        // Process target vertices for downstream fields
                        List<String> downstreamFields = new ArrayList<>(processVertices(datasetJsonObject, vertices, targets, Constants.OUTPUTS_KEY));
                        
                        // Set upstream and downstream fields
                        if (!upstreamFields.isEmpty() && !downstreamFields.isEmpty()) {
                            UrnArray upstreamUrnArray = new UrnArray();
                            for (String fieldUrn : upstreamFields) {
                                upstreamUrnArray.add(Urn.createFromString(fieldUrn));
                            }
                            fgl.setUpstreams(upstreamUrnArray);
                            
                            UrnArray downstreamUrnArray = new UrnArray();
                            for (String fieldUrn : downstreamFields) {
                                downstreamUrnArray.add(Urn.createFromString(fieldUrn));
                            }
                            fgl.setDownstreams(downstreamUrnArray);
                            
                            if (edge.has(Constants.EXPRESSION_KEY)) {
                                fgl.setTransformOperation(edge.get(Constants.EXPRESSION_KEY).getAsString());
                            }
                            
                            fineGrainedLineages.add(fgl);
                        }
                    }
                }
            }
        }
        
        // Set upstreams and fine-grained lineages
        upstreamLineage.setUpstreams(new UpstreamArray(upstreams));
        if (!fineGrainedLineages.isEmpty()) {
            upstreamLineage.setFineGrainedLineages(new FineGrainedLineageArray(fineGrainedLineages));
        }
        
        return upstreamLineage;
    }

    /**
     * Build the data platform instance aspect for the dataset.
     */
    private DataTemplate<DataMap> buildDataPlatformInstanceAspect(JsonObject datasetJsonObject) throws URISyntaxException {
        String platformInstanceUrn = Constants.DATA_PLATFORM_INSTANCE_URN_PREFIX + datasetJsonObject.get(Constants.PLATFORM_INSTANCE_KEY).getAsString() + ")";
        
        return new DataPlatformInstance()
                .setPlatform(new DataPlatformUrn(Constants.PLATFORM_NAME))
                .setInstance(Urn.createFromString(platformInstanceUrn));
    }

    /**
     * Build the subTypes aspect for the dataset.
     */
    private DataTemplate<DataMap> buildSubTypesAspect() {
        return new SubTypes().setTypeNames(new StringArray(Constants.TABLE_KEY));
    }

    /**
     * Build the status aspect for the dataset.
     */
    private DataTemplate<DataMap> buildStatusAspect() {
        return new Status().setRemoved(false);
    }
    
    /**
     * Process vertices to extract field URNs.
     * 
     * @param datasetJsonObject The JSON object representing the dataset
     * @param vertices The array of all vertices
     * @param vertexIds The array of vertex IDs to process
     * @param datasetArrayKey The key for the dataset array ("inputs" or "outputs")
     * @return A list of field URNs
     */
    private List<String> processVertices(JsonObject datasetJsonObject, JsonArray vertices, 
                                        JsonArray vertexIds, String datasetArrayKey) {
        List<String> fieldUrns = new ArrayList<>();
        
        for (var vertexIdElement : vertexIds) {
            int vertexId = vertexIdElement.getAsInt();
            if (vertexId < vertices.size()) {
                JsonObject vertex = vertices.get(vertexId).getAsJsonObject();
                if (Constants.COLUMN_KEY.equals(vertex.get(Constants.VERTEX_TYPE_KEY).getAsString())) {
                    String vertexLabel = vertex.get(Constants.VERTEX_ID_KEY).getAsString();
                    // Create field URN
                    String[] parts = vertexLabel.split("\\.");
                    if (parts.length >= 2) {
                        String tableName = parts[0];
                        String columnName = parts[parts.length - 1];
                        for (var datasetElement : datasetJsonObject.getAsJsonArray(datasetArrayKey)) {
                            String dataset = datasetElement.getAsString();
                            if (dataset.contains(tableName)) {
                                String platformInstance = datasetJsonObject.get(Constants.PLATFORM_INSTANCE_KEY).getAsString();
                                String fieldUrn = Constants.SCHEMA_FIELD_URN_PREFIX + 
                                    Constants.PLATFORM_NAME + "," + platformInstance + "." + dataset + "," + environment + ")," + columnName + ")";
                                fieldUrns.add(fieldUrn);
                            }
                        }
                    }
                }
            }
        }
        
        return fieldUrns;
    }
}
