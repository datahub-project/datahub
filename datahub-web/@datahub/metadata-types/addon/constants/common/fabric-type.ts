/**
 * Fabric group type of internal data center fabrics; We are currently using a name length limit of 10 for FabricType.
 * @export
 * @namespace common
 * @enum {string}
 */
export enum FabricType {
  // Designates DEV fabrics, although it could be machines in EI physical fabric like EI1
  DEV = 'DEV',
  // Designates Early-Integration fabrics, such EI1, EI2 etc.
  EI = 'EI',
  // Designates production fabrics, such as prod-ltx1, prod-lva1 etc.
  PROD = 'PROD',
  // Designates corporation fabrics, such as corp-eat1, corp-lca1 etc.
  CORP = 'CORP',
  // Designates infrastructure testing fabrics, such as lit-lca1-1.
  LIT = 'LIT',
  // Designates Prime fabric group for project Einstein within Linkedin.
  PRIME = 'PRIME',
  // Designates production fabrics deployed in the Mergers and Acquisitions network (MANDA).
  MANDA = 'MANDA',
  // Designates production fabrics deployed in the Azure control plane.
  AZURECONTROL = 'AZURECONTROL',
  // Designates production fabrics deployed in the Azure.
  AZUREPROD = 'AZUREPROD',
  // Designates Early-Integration fabrics deployed in Azure
  AZUREEI = 'AZUREEI'
}
