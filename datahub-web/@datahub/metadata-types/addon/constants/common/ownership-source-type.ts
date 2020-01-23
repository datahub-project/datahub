/**
 * The type of the source
 * @namespace common
 * @export
 * @enum {number}
 */
export enum OwnershipSourceType {
  // Auditing system or audit logs
  Audit = 'AUDIT',
  // Database, e.g. GRANTS table
  Database = 'DATABASE',
  // File system, e.g. file/directory owner
  FileSystem = 'FILE_SYSTEM',
  // Issue tracking system, e.g. Jira
  IssueTrackingSystem = 'ISSUE_TRACKING_SYSTEM',
  // Manually provided by a user
  Manual = 'MANUAL',
  // Other ownership-like service, e.g. Nuage, ACL service etc
  Service = 'SERVICE',
  // SCM system, e.g. GIT, SVN
  SourceControl = 'SOURCE_CONTROL',
  // Other sources
  Other = 'OTHER'
}
