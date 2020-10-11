import { readValuesV2 } from '@datahub/shared/utils/search/values-v2';
import { IFieldValuesResponseV2 } from '@datahub/shared/types/search/fields-v2';
import { debounceAndMemoizeAsyncQuery } from '@datahub/utils/api/autocomplete';

/**
 * Returns a new object for an empty query suggestion response
 * Used as a return value in case the requirement to request data from an endpoint is not met
 */
const getEmptyQueryResultV2 = (): IFieldValuesResponseV2 => ({
  query: '',
  suggestions: []
});

/**
 * It will process Nearly node data and return a string. Sometimes data can be a string array
 * @param data Nearley node data
 */
export const dataToString = (data: Array<string> | string): string => {
  if (typeof data === 'string') {
    return data;
  }

  return data.join('');
};

/**
 * As we need to use this fn on different places of the auto suggestion. Instead of having different caches for each of them
 * we have 1 common fn that will cache the results.
 */
export const facetValuesApiEntities = debounceAndMemoizeAsyncQuery(readValuesV2, {
  defaultResponse: getEmptyQueryResultV2()
});
