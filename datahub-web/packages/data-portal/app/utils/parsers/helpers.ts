import { ISuggestionGroup } from 'datahub-web/utils/parsers/autocomplete/types';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { facetValuesApiEntities } from 'datahub-web/utils/parsers/autocomplete/utils';
import { getFacetForcedValueForEntity } from '@datahub/data-models/entity/utils/facets';
import { FieldValuesRequestV2, IFieldValuesResponseV2 } from '@datahub/shared/types/search/fields-v2';

export const createSuggestionsFromError = (error: string): Array<ISuggestionGroup> => {
  return [
    {
      groupName: ' ',
      options: [
        {
          title: error,
          text: '-',
          disabled: true
        }
      ]
    }
  ];
};

/**
 * Will fetch autocomplete facet values given an entity (feature, metric, dataset).
 */
export const fetchFacetValue = async (
  facetName: string,
  facetValue: string,
  entity: DataModelEntity
): Promise<Array<string>> => {
  // otherwise lets invoke api to fetch values
  let suggestions: Array<string> = [];
  const { search: searchRenderProps, apiEntityName } = entity.renderProps;
  const searchAttributes = searchRenderProps.attributes;
  const fieldMeta = searchAttributes.find((attr): boolean => attr.fieldName === facetName);
  const { minAutocompleteFetchLength } = fieldMeta || { minAutocompleteFetchLength: undefined };
  const cacheKey = `${facetName}:${facetValue}`;
  const forcedFacets = getFacetForcedValueForEntity(searchAttributes);

  // if `facetValue` length (query length) is smaller than the minimum threshold,
  // then an api will not be queried and the default suggestions value ([]) will be returned
  const request: FieldValuesRequestV2<Record<string, string>> = {
    field: facetName,
    input: facetValue,
    type: apiEntityName,
    ...forcedFacets
  };
  const facetValueReturn: IFieldValuesResponseV2 | undefined = await facetValuesApiEntities({
    query: facetValue,
    queryLengthThreshold: minAutocompleteFetchLength,
    cacheKey,
    requestParams: [request]
  });
  suggestions = (facetValueReturn && facetValueReturn.suggestions) || [];

  return suggestions;
};
