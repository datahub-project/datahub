import { makePath } from '@datahub/shared/mirage-addon/helpers/browse/dataset';
import { HandlerFunction, Request, Schema } from 'ember-cli-mirage';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';

export const browsePaths: HandlerFunction = (dbs: Schema, { queryParams: { type, urn } }: Request): Array<string> => {
  if (type === 'dataset') {
    const result = dbs.datasets.where({ uri: urn });
    return result.models.map(model => makePath(model));
  }

  if (type === MockEntity.renderProps.apiEntityName) {
    return ['/mockpath/'];
  }
  return [];
};
