package datahub.hive.producer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Predicate;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx.Index;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Implementation of a post execute hook that logs lineage info to a log file.
 */
public class HiveLineageLogger implements ExecuteWithHookContext {

    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageLogger.class);

    private static final HashSet<String> OPERATION_NAMES = new HashSet<String>();

    private static String CONFIG_PATH = "/etc/hive/conf/hive-lineage-config.xml";

    private static final Configuration configuration = new Configuration();

    private static final ExecutorService executorService;

    private static final KafkaProducerService kafkaProducerService;

    static {
        ExecutorService tempExecutorService = null;
        KafkaProducerService tempKafkaProducerService = null;

        try {
            OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
            loadCustomConfiguration();
            tempExecutorService = getExecutorService();
            LOG.info("Successfully initialized executor service");

            tempKafkaProducerService = getKafkaProducerService();
            LOG.info("Successfully initialized Kafka producer service");

            callExecutorShutdownHook();
        } catch (Throwable t) {
            LOG.warn("Failed to initialize hive lineage hook", t);
        }

        executorService = tempExecutorService;
        kafkaProducerService = tempKafkaProducerService;
    }


    /**
     * An edge in lineage.
     */
    @VisibleForTesting
    public static final class Edge {

        /**
         * The types of Edge.
         */
        public static enum Type {
            PROJECTION, PREDICATE
        }

        private Set<Vertex> sources;
        private Set<Vertex> targets;
        private String expr;
        private Type type;

        Edge(Set<Vertex> sources, Set<Vertex> targets, String expr, Type type) {
            this.sources = sources;
            this.targets = targets;
            this.expr = expr;
            this.type = type;
        }
    }

    /**
     * A vertex in lineage.
     */
    @VisibleForTesting
    public static final class Vertex {

        /**
         * A type in lineage.
         */
        public static enum Type {
            COLUMN, TABLE
        }
        private Type type;
        private String label;
        private int id;

        Vertex(String label) {
            this(label, Type.COLUMN);
        }

        Vertex(String label, Type type) {
            this.label = label;
            this.type = type;
        }

        @Override
        public int hashCode() {
            return label.hashCode() + type.hashCode() * 3;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Vertex)) {
                return false;
            }
            Vertex vertex = (Vertex) obj;
            return label.equals(vertex.label) && type == vertex.type;
        }

        @VisibleForTesting
        public Type getType() {
            return type;
        }

        @VisibleForTesting
        public String getLabel() {
            return label;
        }

        @VisibleForTesting
        public int getId() {
            return id;
        }
    }

    /**
     * A custom thread factory to create threads with a specific naming pattern.
     */
    private static class CustomThreadFactory implements ThreadFactory {
        private final String baseName;
        private final AtomicInteger threadNumber = new AtomicInteger(0);

        public CustomThreadFactory(String baseName) {
            this.baseName = baseName;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, baseName + threadNumber.incrementAndGet());
        }
    }

    /**
     * Initialize the HiveLineageLogger.
     * This method is called by Hive to register the hook.
     */
    @Override
    public void run(HookContext hookContext) {
        try{
            if (executorService == null || kafkaProducerService == null) {
                LOG.info("Executor or Kafka producer service not initialized, skipping hive lineage computation");
                return;
            }
            assert(hookContext.getHookType() == HookType.POST_EXEC_HOOK);
            QueryPlan plan = hookContext.getQueryPlan();
            Index index = hookContext.getIndex();
            SessionState ss = SessionState.get();
            if (ss != null && index != null && OPERATION_NAMES.contains(plan.getOperationName()) && !plan.isExplain()
                    && Boolean.parseBoolean(ss.getConf().get("hive.lineage.hook.info.enabled"))) {
                CompletableFuture.runAsync(() -> computeHiveLineage(plan, index, ss, hookContext), executorService);
            }
        }
        catch(Throwable t){
            LOG.warn("Failed to initialize HiveLineageLogger", t);
        }
    }

    /**
     * Compute the Hive lineage and send it to Kafka.
     * This method is executed asynchronously in a separate thread.
     */
    private void computeHiveLineage(QueryPlan plan, Index index, SessionState ss, HookContext hookContext) {
        long startTime = System.currentTimeMillis();

        try {
            StringBuilderWriter out = new StringBuilderWriter(1024);
            long lineageStartTime = System.currentTimeMillis();
            JsonWriter writer = new JsonWriter(out);

            PlatformConfig platformConfig = getPlatformConfig();
            String environment = platformConfig.environment();
            String platformInstance = platformConfig.platformInstance();
            String formatVersion = platformConfig.formatVersion();

            HashSet<ReadEntity> inputs = plan.getInputs();
            HashSet<WriteEntity> outputs = plan.getOutputs();
            String queryStr = plan.getQueryStr().trim();
            writer.beginObject();
            writer.name("version").value(formatVersion);
            HiveConf conf = ss.getConf();
            boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
            if (!testMode) {
                // Don't emit user/timestamp info in test mode,
                // so that the test golden output file is fixed.
                long queryTime = plan.getQueryStartTime().longValue();
                if (queryTime == 0) queryTime = System.currentTimeMillis();
                long duration = TimeUtils.calculateDuration(queryTime);
                writer.name("user").value(hookContext.getUgi().getUserName());
                writer.name("timestamp").value(queryTime/1000);
                writer.name("duration").value(duration);
                writer.name("jobIds");
                writer.beginArray();
                List<TaskRunner> tasks = hookContext.getCompleteTaskList();
                if (tasks != null && !tasks.isEmpty()) {
                    for (TaskRunner task: tasks) {
                        String jobId = task.getTask().getJobID();
                        if (jobId != null) {
                            writer.value(jobId);
                        }
                    }
                }
                writer.endArray();
            }
            writer.name("engine").value(
                    HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
            writer.name("database").value(ss.getCurrentDatabase());
            writer.name("hash").value(getQueryHash(queryStr));
            writer.name("queryText").value(queryStr);

            writer.name("environment").value(environment);
            writer.name("platformInstance").value(platformInstance);

            writeEntitiesToJson(writer, inputs, "inputs");
            writeEntitiesToJson(writer, outputs, "outputs");

            List<Edge> edges = getEdges(plan, index);
            Set<Vertex> vertices = getVertices(edges);
            writeEdges(writer, edges, hookContext.getConf());
            writeVertices(writer, vertices);
            writer.endObject();
            writer.close();

            long lineageDuration = TimeUtils.calculateDuration(lineageStartTime);
            LOG.info("Time taken for lineage calculation: {} ms", lineageDuration);
            // Logger the lineage info
            String lineage = out.toString();

            if (testMode) {
                // Logger to console
                log(lineage);
            } else {
                // In non-test mode, emit to a log file,
                // which can be different from the normal hive.log.
                // For example, using NoDeleteRollingFileAppender to
                // log to some file with different rolling policy.
                LOG.info("Lineage message (size: {} bytes): {}", lineage.getBytes(java.nio.charset.StandardCharsets.UTF_8).length, lineage);
            }
            LOG.info("Sending lineage on Kafka topic");
            kafkaProducerService.sendMessage(platformInstance, lineage);

        } catch (Throwable t) {
            // Don't fail the query just because of any lineage issue.
            log("Failed to log lineage graph, query is not affected\n"
                    + org.apache.hadoop.util.StringUtils.stringifyException(t));
        } finally{
            long duration = TimeUtils.calculateDuration(startTime);
            LOG.info("Total time taken for lineage calculation and Kafka message sending: {} ms", duration);
        }
    }

    /**
     * Write out a collection of entities to JSON.
     * Only write out table entities.
     */
    private void writeEntitiesToJson(JsonWriter writer, Collection<? extends Entity> entities, String fieldName)
            throws IOException {
        writer.name(fieldName);
        writer.beginArray();
        for (Entity entity : entities) {
            if (entity.getType() == Entity.Type.TABLE) {
                writer.value(entity.getTable().getFullyQualifiedName());
            }
        }
        writer.endArray();
    }

    /**
     * Logger an error to console if available.
     */
    private static void log(String error) {
        LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }
    }

    /**
     * Based on the final select operator, find out all the target columns.
     * For each target column, find out its sources based on the dependency index.
     */
    @VisibleForTesting
    public static List<Edge> getEdges(QueryPlan plan, Index index) {
        LinkedHashMap<String, ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table>> finalSelOps = index.getFinalSelectOps();
        Map<String, Vertex> vertexCache = new LinkedHashMap<String, Vertex>();
        List<Edge> edges = new ArrayList<Edge>();
        for (ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table> pair: finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            SelectOperator finalSelOp = pair.getFirst();
            org.apache.hadoop.hive.ql.metadata.Table t = pair.getSecond();
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {
                destTableName = t.getFullyQualifiedName();
                fieldSchemas = t.getCols();
            } else {
                // Based on the plan outputs, find out the target table name and column names.
                for (WriteEntity output : plan.getOutputs()) {
                    Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE
                            || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destTableName = t.getFullyQualifiedName();
                        List<FieldSchema> cols = t.getCols();
                        if (cols != null && !cols.isEmpty()) {
                            colNames = Utilities.getColumnNamesFromFieldSchema(cols);
                        }
                        break;
                    }
                }
            }
            Map<ColumnInfo, Dependency> colMap = index.getDependencies(finalSelOp);
            List<Dependency> dependencies = colMap != null ? Lists.newArrayList(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (t != null && colMap != null && fields < colMap.size()) {
                // Dynamic partition keys should be added to field schemas.
                List<FieldSchema> partitionKeys = t.getPartitionKeys();
                int dynamicKeyCount = colMap.size() - fields;
                int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int i = 0; i < dynamicKeyCount; i++) {
                        FieldSchema field = partitionKeys.get(keyOffset + i);
                        fieldSchemas.add(field);
                        if (colNames != null) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (dependencies == null || dependencies.size() != fields) {
                log("Result schema has " + fields
                        + " fields, but we don't get as many dependencies");
            } else {
                // Go through each target column, generate the lineage edges.
                Set<Vertex> targets = new LinkedHashSet<Vertex>();
                for (int i = 0; i < fields; i++) {
                    Vertex target = getOrCreateVertex(vertexCache,
                            getTargetFieldName(i, destTableName, colNames, fieldSchemas),
                            Vertex.Type.COLUMN);
                    targets.add(target);
                    Dependency dep = dependencies.get(i);
                    addEdge(vertexCache, edges, dep.getBaseCols(), target,
                            dep.getExpr(), Edge.Type.PROJECTION);
                }
                Set<Predicate> conds = index.getPredicates(finalSelOp);
                if (conds != null && !conds.isEmpty()) {
                    for (Predicate cond: conds) {
                        addEdge(vertexCache, edges, cond.getBaseCols(),
                                new LinkedHashSet<Vertex>(targets), cond.getExpr(),
                                Edge.Type.PREDICATE);
                    }
                }
            }
        }
        return edges;
    }

    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                                Set<BaseColumnInfo> srcCols, Vertex target, String expr, Edge.Type type) {
        Set<Vertex> targets = new LinkedHashSet<Vertex>();
        targets.add(target);
        addEdge(vertexCache, edges, srcCols, targets, expr, type);
    }

    /**
     * Find an edge from all edges that has the same source vertices.
     * If found, add the more targets to this edge's target vertex list.
     * Otherwise, create a new edge and add to edge list.
     */
    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                                Set<BaseColumnInfo> srcCols, Set<Vertex> targets, String expr, Edge.Type type) {
        Set<Vertex> sources = createSourceVertices(vertexCache, srcCols);
        Edge edge = findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }
    }

    /**
     * Convert a list of columns to a set of vertices.
     * Use cached vertices if possible.
     */
    private static Set<Vertex> createSourceVertices(
            Map<String, Vertex> vertexCache, Collection<BaseColumnInfo> baseCols) {
        Set<Vertex> sources = new LinkedHashSet<Vertex>();
        if (baseCols != null && !baseCols.isEmpty()) {
            for(BaseColumnInfo col: baseCols) {
                Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    // Ignore temporary tables
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                String tableName = Warehouse.getQualifiedName(table);
                FieldSchema fieldSchema = col.getColumn();
                String label = tableName;
                if (fieldSchema != null) {
                    type = Vertex.Type.COLUMN;
                    label = tableName + "." + fieldSchema.getName();
                }
                sources.add(getOrCreateVertex(vertexCache, label, type));
            }
        }
        return sources;
    }

    /**
     * Find a vertex from a cache, or create one if not.
     */
    private static Vertex getOrCreateVertex(
            Map<String, Vertex> vertices, String label, Vertex.Type type) {
        Vertex vertex = vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type);
            vertices.put(label, vertex);
        }
        return vertex;
    }

    /**
     * Find an edge that has the same type, expression, and sources.
     */
    private static Edge findSimilarEdgeBySources(
            List<Edge> edges, Set<Vertex> sources, String expr, Edge.Type type) {
        for (Edge edge: edges) {
            if (edge.type == type && StringUtils.equals(edge.expr, expr)
                    && SetUtils.isEqualSet(edge.sources, sources)) {
                return edge;
            }
        }
        return null;
    }

    /**
     * Generate normalized name for a given target column.
     */
    private static String getTargetFieldName(int fieldIndex,
                                             String destTableName, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = fieldSchemas.get(fieldIndex).getName();
        String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName)) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    /**
     * Get all the vertices of all edges. Targets at first,
     * then sources. Assign id to each vertex.
     */
    @VisibleForTesting
    public static Set<Vertex> getVertices(List<Edge> edges) {
        Set<Vertex> vertices = new LinkedHashSet<Vertex>();
        for (Edge edge: edges) {
            vertices.addAll(edge.targets);
        }
        for (Edge edge: edges) {
            vertices.addAll(edge.sources);
        }

        // Assign ids to all vertices,
        // targets at first, then sources.
        int id = 0;
        for (Vertex vertex: vertices) {
            vertex.id = id++;
        }
        return vertices;
    }

    /**
     * Write out an JSON array of edges.
     */
    private void writeEdges(JsonWriter writer, List<Edge> edges, HiveConf conf)
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        writer.name("edges");
        writer.beginArray();
        for (Edge edge: edges) {
            writer.beginObject();
            writer.name("sources");
            writer.beginArray();
            for (Vertex vertex: edge.sources) {
                writer.value(vertex.id);
            }
            writer.endArray();
            writer.name("targets");
            writer.beginArray();
            for (Vertex vertex: edge.targets) {
                writer.value(vertex.id);
            }
            writer.endArray();
            if (edge.expr != null) {
                writer.name("expression").value(HookUtils.redactLogString(conf, edge.expr));
            }
            writer.name("edgeType").value(edge.type.name());
            writer.endObject();
        }
        writer.endArray();
    }

    /**
     * Write out an JSON array of vertices.
     */
    private void writeVertices(JsonWriter writer, Set<Vertex> vertices) throws IOException {
        writer.name("vertices");
        writer.beginArray();
        for (Vertex vertex: vertices) {
            writer.beginObject();
            writer.name("id").value(vertex.id);
            writer.name("vertexType").value(vertex.type.name());
            writer.name("vertexId").value(vertex.label);
            writer.endObject();
        }
        writer.endArray();
    }

    /**
     * Generate query string sha256 hash.
     */
    private String getQueryHash(String queryStr) {
        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putBytes(queryStr.getBytes(Charset.defaultCharset()));
        return hasher.hash().toString();
    }

    /**
     * Load custom configuration from a specified path.
     * If the path is not set, it will fall back to the default configuration path.
     */
    private static void loadCustomConfiguration() {
        try {
            if (SessionState.get() != null && SessionState.get().getConf() != null) {
                String customConfigPath = SessionState.get().getConf().get("hive.lineage.custom.config.path");

                if (customConfigPath != null) {
                    CONFIG_PATH = customConfigPath;
                    LOG.info("Loading configuration from custom path: {}", CONFIG_PATH);
                    configuration.addResource(new Path(CONFIG_PATH));
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to load configuration from custom path: {}", e.getMessage());
            LOG.info("Continuing with configuration from default path");
            loadDefaultConfiguration();
        }
    }

    /**
     * Load the default configuration file.
     * This is used when no custom configuration path is provided.
     */
    private static void loadDefaultConfiguration() {
        try {
            LOG.info("Loading configuration from default path: {}", CONFIG_PATH);
            configuration.addResource(new Path(CONFIG_PATH));
        } catch (Exception e) {
            LOG.warn("Failed to load configuration from default path {}: {}", CONFIG_PATH, e.getMessage());
        }
    }

    /**
     * Get an ExecutorService with a custom thread pool configuration.
     * The thread pool is configured based on the available processors and custom settings.
     */
    private static ExecutorService getExecutorService() {
        ThreadConfig threadConfig = getThreadConfig();
        return new ThreadPoolExecutor(
                threadConfig.corePoolSize(),
                threadConfig.maxPoolSize(),
                threadConfig.keepAliveTime(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(threadConfig.queueCapacity()),
                new CustomThreadFactory(threadConfig.threadName())
        );
    }

    /**
     * Register a shutdown hook to gracefully shut down the executor service and Kafka producer.
     */
    private static void callExecutorShutdownHook() {
        ThreadConfig threadConfig = getThreadConfig();
        // Adding shutdown hook to gracefully shutdown executor when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaProducerService.close();
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(threadConfig.executorServiceTimeout(), TimeUnit.SECONDS)) {
                    LOG.info("Executor service did not terminate within the timeout. Initiating forceful shutdown.");
                    List<Runnable> awaitingTasks = executorService.shutdownNow();
                    LOG.info("Tasks that never commenced execution: {}", awaitingTasks);
                    if (!executorService.awaitTermination(threadConfig.executorServiceTimeout(), TimeUnit.SECONDS))
                        LOG.warn("Executor service did not terminate even after forceful shutdown.");
                }
            } catch (InterruptedException ie) {
                List<Runnable> awaitingTasks = executorService.shutdownNow();
                LOG.warn("Tasks that never commenced execution after interruption: {}", awaitingTasks);
                Thread.currentThread().interrupt();
            }
        }, "HiveLineageShutdownHook"));
    }

    /**
     * Configuration for the thread pool used by the HiveLineageLogger.
     */
    private record ThreadConfig (
            String threadName,
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            int queueCapacity,
            int executorServiceTimeout
    ){}

    /**
     * Get the thread configuration for the HiveLineageLogger.
     * This includes core pool size, max pool size, keep alive time, queue capacity, and executor service timeout.
     */
    private static ThreadConfig getThreadConfig() {
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = Integer.parseInt(configuration.get("hive.lineage.thread.max.pool.size", "100")) * corePoolSize;
        return new ThreadConfig(
                configuration.get("hive.lineage.thread.name", "HiveLineageComputationThread-"),
                corePoolSize,
                maxPoolSize,
                Integer.parseInt(configuration.get("hive.lineage.thread.keep.alive.time", "60")),
                Integer.parseInt(configuration.get("hive.lineage.thread.queue.capacity", "500")),
                Integer.parseInt(configuration.get("hive.lineage.executor.timeout.seconds", "30"))
        );
    }

    /**
     * Get the Kafka producer service configured with the necessary properties.
     * This service is used to send lineage information to a Kafka topic.
     */
    private static KafkaProducerService getKafkaProducerService() {
        KafkaProducerConfig kafkaProducerConfig = getKafkaProducerConfig();
        return new KafkaProducerService(kafkaProducerConfig);
    }

    /**
     * Configuration for the Kafka producer used to send lineage information.
     * This includes properties like bootstrap servers, truststore and keystore locations, topic name, and various timeout settings.
     */
    public record KafkaProducerConfig(
            String bootstrapServers,
            String truststoreLocation,
            String truststorePassword,
            String keystoreLocation,
            String keystorePassword,
            String kafkaTopic,
            int retries, // Controls how many times the Kafka producer will retry sending a record if the initial send fails due to a transient error (network issues, leader not available, etc.)
            int retryBackoffMs, // Amount of time (in milliseconds) that the producer will wait between retry attempts
            boolean enableIdempotence, // Enables idempotent message production, guarantees that even with retries, no duplicate messages will be written to a Kafka topic
            int maxBlockMs, // Maximum amount of time (in milliseconds) that the send() and partitionsFor() methods will block if the producer's buffer is full or metadata is unavailable
            int requestTimeoutMs, // Maximum amount of time (in milliseconds) the producer will wait for a response from the Kafka broker for a request (like sending a message)
            int deliveryTimeoutMs, // Maximum time to wait for a record to be successfully sent (acknowledged by Kafka), including retries, before giving up and failing the send
            int closeTimeoutMs // Maximum time (in milliseconds) to wait for the producer to close gracefully
    ) {}

    /**
     * Get the Kafka producer configuration based on the properties defined in the Hive configuration.
     * This includes environment, platform instance, bootstrap servers, truststore and keystore locations, topic name, and various timeout settings.
     */
    private static KafkaProducerConfig getKafkaProducerConfig() {
        return new KafkaProducerConfig(
                configuration.get("hive.lineage.kafka.bootstrap.servers"),
                configuration.get("hive.lineage.kafka.ssl.truststore.location"),
                configuration.get("hive.lineage.kafka.ssl.truststore.password"),
                configuration.get("hive.lineage.kafka.ssl.keystore.location"),
                configuration.get("hive.lineage.kafka.ssl.keystore.password"),
                configuration.get("hive.lineage.kafka.topic", "HiveLineage_v1"),
                Integer.parseInt(configuration.get("hive.lineage.kafka.retries", "0")),
                Integer.parseInt(configuration.get("hive.lineage.kafka.retry.backoff.ms", "100")),
                Boolean.parseBoolean(configuration.get("hive.lineage.kafka.enable.idempotence", "false")),
                Integer.parseInt(configuration.get("hive.lineage.kafka.max.block.ms", "3000")),
                Integer.parseInt(configuration.get("hive.lineage.kafka.request.timeout.ms", "3000")),
                Integer.parseInt(configuration.get("hive.lineage.kafka.delivery.timeout.ms", "5000")),
                Integer.parseInt(configuration.get("hive.lineage.kafka.close.timeout.ms", "30000"))
        );
    }

    /**
     * Configuration for the platform instance, including environment and format version.
     * This is used to provide context for the lineage information being logged.
     */
    private record PlatformConfig(
            String environment,
            String platformInstance,
            String formatVersion
    ) {}

    /**
     * Get the platform configuration based on the properties defined in the Hive configuration.
     * This includes environment, platform instance, and format version.
     */
    private static PlatformConfig getPlatformConfig() {
        return new PlatformConfig(
                configuration.get("hive.lineage.environment"),
                configuration.get("hive.lineage.platform.instance"),
                configuration.get("hive.lineage.format.version", "1.0")
        );
    }
}
