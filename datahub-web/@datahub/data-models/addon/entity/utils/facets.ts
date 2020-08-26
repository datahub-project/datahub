import { typeOf } from '@ember/utils';
import { IFacetsSelectionsMap, IFacetSelections } from '@datahub/data-models/types/entity/facets';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';
import { KeyNamesWithValueType } from '@datahub/utils/types/base';

/**
 * Filtering field names that returns array
 */
type FieldNames = KeyNamesWithValueType<ISearchEntityRenderProps, Array<string> | undefined>;

/**
 * returns key/value pairs depending on key of field
 * @param fields
 * @param filterField
 */
const facetFilter = (
  fields: Array<ISearchEntityRenderProps>,
  filterField: FieldNames
): Record<string, Array<string>> => {
  return fields.reduce((facetsApiParams: Record<string, Array<string>>, field): Record<string, Array<string>> => {
    if (filterField && typeOf(field[filterField]) !== 'undefined') {
      return {
        ...facetsApiParams,
        [field.fieldName]: field[filterField] || []
      };
    }
    return facetsApiParams;
  }, {});
};

/**
 * Will return an key value object with forced facets given a list of fields
 */
export const getFacetForcedValueForEntity = (fields: Array<ISearchEntityRenderProps>): Record<string, Array<string>> =>
  facetFilter(fields, 'forcedFacetValue');

/**
 * Will return an key value object with deafult facets given a list of fields
 */
export const getFacetDefaultValueForEntity = (fields: Array<ISearchEntityRenderProps>): Record<string, Array<string>> =>
  facetFilter(fields, 'facetDefaultValue');

/**
 * Transforms an input like this:
 * {
 *  status: ['PUBLISHED']
 * }
 *
 * into this:
 * {
 *  status: {
 *    PUBLISHED: true
 *  }
 * }
 *
 * This is useful to transform default facets into a string
 * @param defaults
 */
export const transformDefaultsIntoSelections = (defaults: Record<string, Array<string>>): IFacetsSelectionsMap =>
  Object.keys(defaults).reduce((selections: IFacetsSelectionsMap, facetKey: string): IFacetsSelectionsMap => {
    const values = defaults[facetKey];
    return {
      ...selections,
      [facetKey]: values.reduce(
        (selection: IFacetSelections, value: string): IFacetSelections => ({ ...selection, [value]: true }),
        {}
      )
    };
  }, {});
