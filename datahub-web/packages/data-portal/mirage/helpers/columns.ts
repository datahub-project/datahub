import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

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

export { getDatasetSchema };
