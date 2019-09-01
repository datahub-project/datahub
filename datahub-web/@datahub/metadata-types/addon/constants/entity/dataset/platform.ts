/**
 * The known/supported list of dataset platforms
 * @enum {string}
 */
export enum DatasetPlatform {
  Kafka = 'kafka',
  KafkaLc = 'kafka-lc',
  Presto = 'presto',
  Espresso = 'espresso',
  Oracle = 'oracle',
  MySql = 'mysql',
  Teradata = 'teradata',
  HDFS = 'hdfs',
  SEAS_HDFS = 'seas-hdfs',
  SEAS_DEPLOYED = 'seas-deployed',
  Ambry = 'ambry',
  Couchbase = 'couchbase',
  Voldemort = 'voldemort',
  Venice = 'venice',
  Vector = 'vector',
  Hive = 'hive',
  FollowFeed = 'followfeed',
  UMP = 'ump',
  Pinot = 'pinot',
  Dalids = 'dalids'
}
