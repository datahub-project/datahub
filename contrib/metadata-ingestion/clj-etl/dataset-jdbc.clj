(require '[clojure.edn :as edn])
(require '[clojure.pprint :as pprint])
(require '[clojure.java.io :as io])
(require '[clojure.java.jdbc :as j])
(require '[cheshire.core :as json])
(require '[net.cgrand.xforms :as x])


(import '[org.apache.avro Schema$Parser])
(import '[org.apache.avro.io DecoderFactory])
(import '[org.apache.avro.generic GenericDatumReader])

(import '[org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
(import '[io.confluent.kafka.serializers KafkaAvroSerializer])

(def ora-columns-query
  " select
      c.OWNER || '.' || c.TABLE_NAME as schema_name
    , t.COMMENTS as schema_description
    , c.COLUMN_NAME as field_path
    , c.DATA_TYPE as native_data_type 
    , m.COMMENTS as description
    from ALL_TAB_COLUMNS c
      left join ALL_TAB_COMMENTS t
        on c.OWNER = t.OWNER
        and c.TABLE_NAME = t.TABLE_NAME
      left join ALL_COL_COMMENTS m
        on c.OWNER = m.OWNER
        and c.TABLE_NAME = m.TABLE_NAME
        and c.COLUMN_NAME = m.COLUMN_NAME
    where NOT REGEXP_LIKE(c.OWNER, 'ANONYMOUS|PUBLIC|SYS|SYSTEM|DBSNMP|MDSYS|CTXSYS|XDB|TSMSYS|ORACLE.*|APEX.*|TEST?*|GG_.*|\\$')
    order by schema_name, c.COLUMN_ID" )

(def mysql-columns-query
  " select 
      concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) as schema_name
    , NULL as schema_description
    , c.COLUMN_NAME as field_path
    , c.DATA_TYPE as native_data_type
    , c.COLUMN_COMMENT as description
    from information_schema.columns c
    where table_schema not in ('information_schema') 
    order by schema_name, c.ORDINAL_POSITION" )

(def pg-columns-query "")

(def db-query-mapping 
  { "oracle:thin" ora-columns-query 
    "mysql" mysql-columns-query })

(defn find-db-query [{dbtype :dbtype}]  (db-query-mapping dbtype) )

(defn mk-mce-json [origin data-platform schema-data]
  (let [ schema-name (-> schema-data first :schema_name) 
         schema-description (-> schema-data first :schema_description) ]
    (json/generate-string 
      { "auditHeader" nil
        "proposedSnapshot" 
          { "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot"
           , { "urn" (format "urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s)" data-platform schema-name origin) 
             , "aspects"
                 [ { "com.linkedin.pegasus2avro.dataset.DatasetProperties" 
                     { "description" {"string" schema-name}
                       "uri" nil
                       "tags" []
                       "customProperties" {}
                      }}
                 , { "com.linkedin.pegasus2avro.common.Status" {"removed" false}}
                 , { "com.linkedin.pegasus2avro.common.Ownership"
                     { "owners" [{"owner" "urn:li:corpuser:datahub", "type" "DEVELOPER", "source" nil}]
                     , "lastModified" {"time" 0, "actor" "urn:li:corpuser:datahub", "impersonator" nil}}} 
                 , { "com.linkedin.pegasus2avro.schema.SchemaMetadata"
                     { "schemaName" schema-name
                     , "platform" (format "urn:li:dataPlatform:%s" data-platform) 
                     , "version" 0
                     , "created" {"time" 0, "actor" "urn:li:corpuser:datahub", "impersonator" nil}
                     , "lastModified" {"time" 0, "actor" "urn:li:corpuser:datahub", "impersonator" nil}
                     , "deleted" nil
                     , "dataset" nil
                     , "cluster" nil
                     , "hash" ""
                     , "platformSchema" {"com.linkedin.pegasus2avro.schema.EspressoSchema" 
                                          {"documentSchema" "{}", "tableSchema" "{}"} }
                     , "fields"
                         (for [{ fieldPath :field_path 
                                   description :description
                                   nativeDataType :native_data_type } schema-data]
                           { "fieldPath" fieldPath
                             , "jsonPath" nil
                             , "nullable" false
                             , "description" (when (some? description) {"string" description} ) 
                             , "type" {"type" {"com.linkedin.pegasus2avro.schema.StringType" {}}}
                             , "nativeDataType" nativeDataType
                             , "recursive" false} )
                     , "primaryKeys" nil
                     , "foreignKeysSpecs" nil
                       }} 
                 ]} }
        "proposedDelta" nil }) ))

(defn json->avro [schema-file m]
  (let [schema (.parse (new Schema$Parser) (io/file schema-file))
        reader (new GenericDatumReader schema)]
    (->> m (.jsonDecoder (DecoderFactory/get) schema) (.read reader nil))) )

(defn load-selector-conf []
  (when (not= (count *command-line-args*) 1) 
    (println "** the selector paramter is missing!")
    (System/exit 1) )
  (let [selector (edn/read-string (first *command-line-args*))
        conf (-> "./dataset-jdbc.conf.edn" slurp edn/read-string selector)] 
    (when (nil? conf) 
      (println "** the selector conf is missing!")
      (System/exit 1)) 
    (pprint/pprint conf)
    conf ))

(defn test-conf []
    { :in { :db { :dbtype "oracle:thin" :dbname "EDWDB" :host "10.129.35.227" :user "comm" :password "comm" } }
      :params {:origin "PROD" }
      :out { :kafka { "bootstrap.servers" "10.132.37.201:9092"
                    , "schema.registry.url" "http://10.132.37.201:8081" }} } )

(println "starting...")
(def mce-schema "../MetadataChangeEvent.avsc")

(let [ conf (load-selector-conf) 
       ; conf (test-conf)
       db (get-in conf [:in :db])
       origin (get-in conf [:params :origin])
       data-platform (or (get-in conf [:params :data-platform]) (get-in conf [:in :db :dbname]))
       kafka-conf (merge (get-in conf [:out :kafka]) 
                         { "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                         , "value.serializer" "io.confluent.kafka.serializers.KafkaAvroSerializer" }) 
       _ (println "** kafka-conf" kafka-conf)
       prop (doto (new java.util.Properties) (.putAll kafka-conf)) 
       kp (new KafkaProducer prop)
       kafka-send (fn [rec] (-> kp (.send (new ProducerRecord "MetadataChangeEvent" rec) ) ) )]
    (println "** params: " [:origin origin :data-platform data-platform])
    ((comp doall sequence) 
      (comp
          (partition-by :schema_name)
          (map (fn [schema-cols] (println "schema_name: " (-> schema-cols first :schema_name)) schema-cols))
          (map (partial mk-mce-json origin data-platform) )
          (map (partial json->avro mce-schema) )
          (map kafka-send)
          x/count
          (map (partial println "total table num: "))
          )
      (j/query db (find-db-query db)) )
    (.flush kp) )

(println "finished") 

