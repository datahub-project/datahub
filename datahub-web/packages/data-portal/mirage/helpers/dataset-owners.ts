import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

const getDatasetOwners = function(this: IFunctionRouteHandler, { owners }: { owners: any }) {
  return {
    owners: this.serialize(owners.all())
  };
};

export { getDatasetOwners };
