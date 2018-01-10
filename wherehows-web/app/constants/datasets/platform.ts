/**
 * The known/supported list of dataset platforms
 * @enum {string}
 */
enum DatasetPlatform {
  Kafka = 'kafka',
  Espresso = 'espresso',
  Oracle = 'oracle',
  MySql = 'mysql',
  Teradata = 'teradata',
  HDFS = 'hdfs',
  Ambry = 'ambry',
  Couchbase = 'couchbase',
  Voldemort = 'voldemort',
  Venice = 'venice',
  Hive = 'hive'
}

export { DatasetPlatform };
