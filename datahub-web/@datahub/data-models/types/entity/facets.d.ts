import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

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
  // the business side logical name of the facet
  name: string;
  // the visually represented name of the facet
  displayName: string;
  // Array of key value pairs that contain the actual search facet and it's value
  values: Array<ISearchFacetOption>;
  // Optional component that takes care of all header based display logic like : header text , additional helpers , other UI modifiers like checkboxes or edit options
  headerComponent?: IDynamicComponent;
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
