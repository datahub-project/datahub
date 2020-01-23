/**
 * Options inside a facet
 * @interface ISearchFacetOption
 */
export interface ISearchFacetOption {
  value: string;
  label: string;
  count: number;
}

/**
 * Interface of a facet
 */
export interface ISearchFacet {
  name: string;
  displayName: string;
  values: Array<ISearchFacetOption>;
}

/**
 * Dynamic facet selections
 */
export interface IFacetSelections {
  [key: string]: boolean;
}

/**
 * Dynamic facets:
 * {
 *  source: {
 *    hdfs: true,
 *    hive: true
 *  }
 * }
 */
export interface IFacetsSelectionsMap {
  [key: string]: IFacetSelections;
}

/**
 * Compressed version of selection to put it in a url
 * {
 *  source: ['hdfs', 'hive]
 * }
 */
export interface IFacetsSelectionsArray {
  [key: string]: Array<string>;
}

/**
 * Dynamic counts facet option
 */
export interface IFacetCounts {
  [key: string]: number;
}

/**
 * Dynamic counts facet similar to selections
 */
export interface IFacetsCounts {
  [key: string]: IFacetCounts;
}
