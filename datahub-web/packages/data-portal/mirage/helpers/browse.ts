import { IBrowseResponse, IBrowseParams } from '@datahub/data-models/types/entity/browse';
import { IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';

export const browse = (_dbs: IMirageWherehowsDBs, _request: IMirageRequest<IBrowseParams<{}>>): IBrowseResponse => {
  let groups: IBrowseResponse['metadata']['groups'] = [];

  return { metadata: { path: '', totalNumEntities: 0, groups }, elements: [], start: 0, total: 0, count: 0 };
};
