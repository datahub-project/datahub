import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetOwners = function(this: IFunctionRouteHandler, { owners }: { owners: any }) {
  return {
    owners: this.serialize(owners.all())
  };
};

export { getDatasetOwners };
