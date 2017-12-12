/**
 * The known/supported list of dataset platforms
 * @enum {string}
 */
enum DatasetPlatform {
  Kafka = 'KAFKA',
  Espresso = 'ESPRESSO',
  Oracle = 'ORACLE',
  MySql = 'MYSQL',
  Teradata = 'TERADATA',
  HDFS = 'HDFS',
  Ambry = 'AMBRY',
  Couchbase = 'COUCHBASE',
  Voldemort = 'VOLDEMORT',
  Venice = 'VENICE',
  Hive = 'HIVE'
}

export { DatasetPlatform };
