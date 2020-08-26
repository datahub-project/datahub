/**
 * Properties related to a Voldemort source
 * @export
 * @interface IVoldemortSourceProperties
 */
export interface IVoldemortSourceProperties {
  // Voldemort Store name
  storeName: string;
  // MVEL key expression
  keyExpression: string;
  // The connection point for the voldemort store
  bootstrapUrl: string;
}
