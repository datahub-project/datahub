import { ISuggestionGroup } from 'wherehows-web/utils/parsers/autocomplete/types';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { IFieldValuesResponseV2, FieldValuesRequestV2 } from 'wherehows-web/typings/app/search/fields-v2';
import { facetValuesApiEntities } from 'wherehows-web/utils/parsers/autocomplete/utils';

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
  const searchRenderProps = entity.renderProps.search;
  const searchAttributes = searchRenderProps.attributes;
  const fieldMeta = searchAttributes.find((attr): boolean => attr.fieldName === facetName);
  const { minAutocompleteFetchLength } = fieldMeta || { minAutocompleteFetchLength: undefined };
  const cacheKey = `${facetName}:${facetValue}`;

  // if `facetValue` length (query length) is smaller than the minimum threshold,
  // then an api will not be queried and the default suggestions value ([]) will be returned
  const request: FieldValuesRequestV2<Record<string, string>> = {
    field: facetName,
    input: facetValue,
    type: searchRenderProps.apiName
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
