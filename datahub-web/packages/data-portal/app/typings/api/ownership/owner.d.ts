/**
 * Information of a shared owner
 * @interface ISharedOwner
 */
export interface ISharedOwner {
  // Dataset owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:abc, and urn:li:multiProduct:mp_name
  owner: string;
  // The category of the owner role
  ownerCategory: OwnerCategory;
  // From where the owner info is extracted
  ownershipProvider: OwnershipProvider;
  // A URL indicating the source of this suggested ownership information
  sourceUrl: string;
}

/**
 * Owner category or owner role
 * @enum string
 */
export enum OwnerCategory {
  // Who owns the schema or DDL
  DataOwner = 'DATA_OWNER',
  // Service or code owner of the service which produces/generates data records
  Producer = 'PRODUCER',
  // DBA, SRE, ETL Replicator who can setup and modify the data storage
  Delegate = 'DELEGATE',
  // Business owner or product manager/owner
  Stakeholder = 'STAKEHOLDER',
  // Service or code owner of the service which consumes data records
  Consumer = 'CONSUMER'
}

/**
 * Source/provider of the ownership information
 * @enum string
 */
export enum OwnershipProvider {
  // Auditing system or audit logs
  Audit = 'AUDIT',
  // Data vault system
  DataVault = 'DATA_VAULT',
  // Database, e.g. GRANTS table
  Database = 'DATABASE',
  // File system, e.g. file/directory owner
  FileSystem = 'FILE_SYSTEM',
  // Jira tickets
  Jira = 'JIRA',
  // Nuage service
  Nuage = 'NUAGE',
  // Other sources
  Other = 'OTHER',
  // Review board submitter, reviewer
  ReviewBoard = 'REVIEW_BOARD',
  // Software ownership service
  Sos = 'SOS',
  // SCM system: gitli, svn or github
  SourceControl = 'SOURCE_CONTROL',
  // User input on UI
  UserInterface = 'USER_INTERFACE'
}
