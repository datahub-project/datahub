/**
 * Api expected request params for suggestion values autocomplete
 */
export interface IFieldValuesRequest {
  input: string;
  facet?: string;
}

/**
 * Api expected response for suggestion values autocomplete
 */
export interface IFieldValuesResponse {
  input: string;
  source: Array<string>;
}
