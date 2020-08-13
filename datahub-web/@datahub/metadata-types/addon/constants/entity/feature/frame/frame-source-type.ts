/**
 * Frame feature source type
 * @export
 * @enum {number}
 */
export enum FrameSourceType {
  // Features that are generated from a HDFS source dataset
  Hdfs = 'HDFS',
  // Features that are generated from an Espresso source
  Espresso = 'ESPRESSO',
  // Features that are generated from a Voldemort source
  Voldemort = 'VOLDEMORT',
  // Features that are generated from a Restli source
  RestLi = 'RESTLI',
  // Features that are generated from a Venice source
  Venice = 'VENICE',
  // Features that are generated using a passthrough source
  Passthrough = 'PASSTHROUGH'
}
