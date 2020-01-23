import { ApiStatus } from '@datahub/utils/api/shared';

const getSearchResults = function(_: any, request: any) {
  return {
    status: ApiStatus.OK,
    result: {
      keywords: request.queryParams.keyword,
      data: [],
      groupbydataorigin: {
        prod: 1,
        corp: 1,
        ei: 1
      }
    }
  };
};

export { getSearchResults };
