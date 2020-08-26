import { IBrowseResponse } from '@datahub/data-models/types/entity/browse';
import { groupBy } from 'lodash';
import { Schema } from 'ember-cli-mirage';

interface IBrowseTemporalObject {
  path: string;
  remainder: string;
  next: string;
  isGroup: boolean;
  model: Com.Linkedin.Dataset.Dataset;
}

export const makePath = (model: Com.Linkedin.Dataset.Dataset): string =>
  `/${model.platform}/${model.name.split('.').join('/')}`.replace(/\/\//gi, '/');

/**
 * Will convert db models into a an structure that will help us with grouping and filtering
 */
const toTempObject = (models: Array<Com.Linkedin.Dataset.Dataset>, filterPath: string): Array<IBrowseTemporalObject> =>
  models
    .map(model => {
      const path = makePath(model);
      const remainder = path.replace(filterPath, '');
      const parts = remainder.split('/').filter(Boolean);
      return {
        path,
        remainder,
        next: parts[0],
        isGroup: parts.length > 1,
        model
      };
    })
    .filter(vo => vo.path.indexOf(filterPath) === 0);

/**
 * Will read and filter all dataset given a path and output the next browsing paths
 */
export const browseDatasets = (dbs: Schema, filterPath: string | undefined = ''): IBrowseResponse => {
  const result = dbs.datasets.all();

  const tempObjects = toTempObject(result.models, filterPath);
  const grouped = groupBy(
    tempObjects.filter(vo => vo.isGroup),
    'next'
  );
  const groups = Object.keys(grouped).map(groupKey => ({
    name: groupKey,
    count: grouped[groupKey].length
  }));
  const elements = tempObjects
    .filter(vo => !vo.isGroup)
    .map(vo => {
      return {
        urn: vo.model.uri || '',
        name: vo.next
      };
    });
  return {
    metadata: { path: filterPath, totalNumEntities: tempObjects.length, groups },
    elements: elements,
    start: 0,
    total: tempObjects.length,
    count: 100
  };
};
