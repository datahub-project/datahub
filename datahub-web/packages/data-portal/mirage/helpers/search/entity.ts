import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import searchResponse from 'wherehows-web/mirage/fixtures/search-response';
import { IEntitySearchResult, IAggregationMetadata } from 'wherehows-web/typings/api/search/entity';

export const getEntitySearchResults = function(
  this: IFunctionRouteHandler,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  schema: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  request: any
): IEntitySearchResult<any> {
  const { input, type } = request.queryParams;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let model: any;
  let modelName: string;

  if (type === DatasetEntity.renderProps.search.apiName) {
    model = 'datasetViews';
    modelName = 'name';
  }

  if (!model) {
    throw new Error('No entity by that name: ' + input);
  }

  const models = schema[model];
  const serializedModels =
    input === searchResponse.result.keywords ? searchResponse.result.data : this.serialize(models.all());

  const data = serializedModels
    // filter by entity name or return all if searchquery is 'owners:'
    .filter((item: any) => item[modelName].includes(input) || input.startsWith('owners:'))
    .map((entity: any) => {
      return entity;
    });

  let metas: Array<IAggregationMetadata> = [];

  return {
    elements: data,
    start: 0,
    count: data.length,
    total: data.length,
    searchResultMetadatas: metas
  };
};
