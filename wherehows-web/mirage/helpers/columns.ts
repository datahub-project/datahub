import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetColumns = function(this: IFunctionRouteHandler, { columns }: { columns: any }) {
  return {
    columns: this.serialize(columns.all()),
    schemaless: false,
    status: ApiStatus.OK
  };
};

const getDatasetSchema = function(this: IFunctionRouteHandler, { columns }: { columns: any }) {
  return {
    schema: {
      columns: this.serialize(columns.all()),
      schemaless: false,
      keySchema: null,
      rawSchema: '{}'
    }
  };
};

export { getDatasetColumns, getDatasetSchema };
