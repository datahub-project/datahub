/**
 * Api expected request params for suggestion values autocomplete
 *
 * There can be some extra filters in the call depending on the entity.
 * For example, for features we need to pass {status: 'PUBLISHED'};
 */
export type FieldValuesRequestV2<T> = {
  input: string;
  field: string;
  type: string;
} & T;

/**
 * Api expected response for suggestion values autocomplete
 */
export interface IFieldValuesResponseV2 {
  query: string;
  suggestions: Array<string>;
}
