import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getSearchResults = function(_: any, request: any) {
  return {
    status: ApiStatus.OK,
    result: {
      keywords: request.queryParams.keyword,
      data: []
    }
  };
};

export { getSearchResults };
