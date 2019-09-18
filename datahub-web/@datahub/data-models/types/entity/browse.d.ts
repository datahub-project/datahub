/**
 * Browse api will return a list of metrics that will have
 * this structure.
 * Additional filters can be used for browse. Any field in the entity document could be used.
 * This will be used as a filter to narrow down browse results. E.g.: status=PUBLISHED for features
 * see: https://docs.google.com/document/d/1KbX97Sk8vQgaE1ksYvXyUc7Mf2PAgg311JBwZi-hgH8
 */
export type IBrowseParams<T> = {
  type: string;
  path?: string;
  start?: number;
  count?: number;
} & T;

/**
 *
 */
export interface IBrowseResponse {
  elements: Array<{ urn: string; name: string }>;
  start: number;
  count: number;
  total: number;
  metadata: {
    totalNumEntities: number;
    path: string;
    groups: Array<{ name: string; count: number }>;
  };
}

/**
 * Path for bowse paths
 */
export interface IBrowsePathParams {
  type: string;
  urn: string;
}
