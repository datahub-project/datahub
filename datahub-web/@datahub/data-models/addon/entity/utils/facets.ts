import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { typeOf } from '@ember/utils';
import { IFacetsSelectionsMap, IFacetSelections } from '@datahub/data-models/types/entity/facets';

/**
 * Will return an key value object with forced facets given a list of fields
 */
export const getFacetDefaultValueForEntity = (
  fields: Array<ISearchEntityRenderProps>
): Record<string, Array<string>> => {
  return fields.reduce((facetsApiParams: Record<string, Array<string>>, field): Record<string, Array<string>> => {
    if (typeOf(field.facetDefaultValue) !== 'undefined') {
      return {
        ...facetsApiParams,
        [field.fieldName]: field.facetDefaultValue || []
      };
    }
    return facetsApiParams;
  }, {});
};

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
