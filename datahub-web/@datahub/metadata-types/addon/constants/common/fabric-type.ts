/**
 * Fabric group type of internal data center fabrics; We are currently using a name length limit of 10 for FabricType.
 * @export
 * @namespace common
 * @enum {string}
 */
export enum FabricType {
  // Designates DEV fabrics
  DEV = 'DEV',
  // Designates Early-Integration fabrics
  EI = 'EI',
  // Designates production fabrics
  PROD = 'PROD',
  // Designates corporation fabrics
  CORP = 'CORP'
}
