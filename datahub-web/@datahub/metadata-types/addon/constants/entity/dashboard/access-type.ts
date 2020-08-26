// Access level of the dashboard. It has impact on visibility in search results
export enum AccessType {
  // Team certified dashboard, which is visible from search results.
  CERTIFIED = 'CERTIFIED',
  // Public accessible dashboard, which is visible from search results.
  PUBLIC = 'PUBLIC',
  // Private accessible dashboard, which is not visible from search results.
  PRIVATE = 'PRIVATE'
}
