/**
 * Standard paginated API response
 */
export interface IPaginatedResponse<T> {
  // Returned elements
  elements: Array<T>;
  // Starting index of the list
  start: number;
  // Number of returned results
  count: number;
  // Total number of results available
  total: number;
}
