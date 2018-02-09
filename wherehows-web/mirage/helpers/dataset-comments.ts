import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetComments = function(this: IFunctionRouteHandler, { comments }: { comments: any }) {
  return {
    data: {
      comments: this.serialize(comments.all()),
      count: 2,
      itemsPerPage: 10,
      page: 1,
      totalPages: 1
    },
    status: ApiStatus.OK
  };
};

export { getDatasetComments };
