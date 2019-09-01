import { IMirageRequest, IMirageDB } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

type KeyOfEntity = keyof IDatasetView;
type AvailableEntity = IDatasetView;

interface IAutocompleteDatasetRequest {
  input: string;
  facet?: KeyOfEntity;
}

interface IAutocompleteRequest {
  input: string;
  field?: KeyOfEntity;
  type: 'metric' | 'dataset';
}

const getDbFromType = (type: IAutocompleteRequest['type'], dbs: IMirageWherehowsDBs): IMirageDB<AvailableEntity> => {
  switch (type) {
    case DatasetEntity.renderProps.search.apiName:
      return dbs.datasetViews;
  }
  throw new Error(`Entity ${type} not defined in mirage`);
};

const getStringValue = (entity: AvailableEntity, field: KeyOfEntity): string =>
  `${entity[field as keyof typeof entity] || ''}`;

const getSuggestions = (
  facet: KeyOfEntity,
  value: string,
  entity: IAutocompleteRequest['type'],
  dbs: IMirageWherehowsDBs
): Array<string> => {
  const db = getDbFromType(entity, dbs);
  return db
    .all()
    .models.filter(entity => getStringValue(entity, facet).indexOf(value) >= 0)
    .map(entity => getStringValue(entity, facet))
    .slice(0, 10);
};

export const getAutocompleteDatasets = function(
  dbs: IMirageWherehowsDBs,
  request: IMirageRequest<IAutocompleteDatasetRequest>
) {
  const { facet = 'name', input } = request.queryParams;

  return {
    status: 'ok',
    input: input,
    source: getSuggestions(facet, input, 'dataset', dbs)
  };
};

export const getAutocomplete = function(dbs: IMirageWherehowsDBs, request: IMirageRequest<IAutocompleteRequest>) {
  const { field = 'name', input, type } = request.queryParams;

  return {
    status: 'ok',
    query: input,
    suggestions: getSuggestions(field, input, type, dbs)
  };
};
