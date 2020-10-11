import { getJSON } from '@datahub/utils/api/fetcher';
import { FieldValuesRequestV2, IFieldValuesResponseV2 } from '@datahub/shared/types/search/fields-v2';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import buildUrl from '@datahub/utils/api/build-url';

/**
 * Autocomplete field values for suggestions
 * constructing url
 * @param params
 */
export const fieldsUrl = <T>(params: FieldValuesRequestV2<T>): string => {
  return buildUrl(`${getApiRoot(ApiVersion.v2)}/autocomplete`, params);
};

/**
 * Autocomplete field values for suggestions
 * invoking api
 * @param params
 */
export const readValuesV2 = <T>(params: FieldValuesRequestV2<T>): Promise<IFieldValuesResponseV2> =>
  getJSON<IFieldValuesResponseV2>({ url: fieldsUrl<T>(params) });
