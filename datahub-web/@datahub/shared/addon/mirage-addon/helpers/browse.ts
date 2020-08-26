import { IBrowseResponse } from '@datahub/data-models/types/entity/browse';
import { HandlerFunction, Schema, Request } from 'ember-cli-mirage';
import { browseDatasets } from '@datahub/shared/mirage-addon/helpers/browse/dataset';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';

export const browse: HandlerFunction = (dbs: Schema, request: Request): IBrowseResponse => {
  const {
    queryParams: { path = '', type }
  } = request;

  if (type === 'dataset') {
    return browseDatasets(dbs, path);
  }

  if (type === MockEntity.renderProps.apiEntityName) {
    return {
      metadata: { path: 'MockPath', totalNumEntities: 1, groups: [] },
      elements: [{ name: 'mock1', urn: 'someurn' }],
      start: 0,
      total: 1,
      count: 1
    };
  }

  return { metadata: { path: '', totalNumEntities: 0, groups: [] }, elements: [], start: 0, total: 0, count: 0 };
};
