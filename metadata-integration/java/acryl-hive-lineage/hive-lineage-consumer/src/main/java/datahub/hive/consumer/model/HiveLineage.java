package datahub.hive.consumer.model;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.List;

/**
 * Model class representing the Hive lineage data received from Kafka.
 */
@Data
public class HiveLineage {
    private String version;
    private String user;
    private long timestamp;
    private long duration;
    
    @SerializedName("jobIds")
    private List<String> jobIds;
    
    private String engine;
    private String database;
    private String hash;
    
    @SerializedName("queryText")
    private String queryText;
    
    private String environment;
    
    @SerializedName("platformInstance")
    private String platformInstance;
    
    private List<String> inputs;
    private List<String> outputs;
    private List<Edge> edges;
    private List<Vertex> vertices;

    /**
     * Edge in the lineage graph.
     */
    @Data
    public static class Edge {
        private List<Integer> sources;
        private List<Integer> targets;
        private String expression;
        
        @SerializedName("edgeType")
        private String edgeType;
    }

    /**
     * Vertex in the lineage graph.
     */
    @Data
    public static class Vertex {
        private int id;
        
        @SerializedName("vertexType")
        private String vertexType;
        
        @SerializedName("vertexId")
        private String vertexId;
    }
}
