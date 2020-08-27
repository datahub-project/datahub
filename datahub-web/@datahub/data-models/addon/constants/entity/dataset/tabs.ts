import { CommonTabProperties } from '@datahub/data-models/constants/entity/shared/tabs';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * Lists the dataset tabs available
 * @export
 * @enum {string}
 */
export enum DatasetTab {
  Properties = 'properties',
  Access = 'access',
  Schema = 'schema',
  Ownership = 'ownership',
  Compliance = 'compliance',
  Relationships = 'relationships',
  Health = 'health',
  DatasetGroups = 'datasetgroups'
}

/**
 * Properties for dataset tabs
 */
export const TabProperties: Array<ITabProperties> = [
  {
    id: DatasetTab.Schema,
    title: 'Schema',
    contentComponent: 'datasets/containers/dataset-schema'
  },
  {
    id: DatasetTab.Properties,
    title: 'Status',
    contentComponent: 'datasets/containers/dataset-properties'
  },
  {
    id: DatasetTab.Access,
    title: 'ACL Access',
    contentComponent: 'jit-acl/containers/jit-acl-access-container',
    lazyRender: true
  },
  {
    id: DatasetTab.Ownership,
    title: 'Ownership',
    contentComponent: 'datasets/containers/dataset-ownership'
  },
  {
    id: DatasetTab.Compliance,
    title: 'Compliance',
    contentComponent: 'datasets/containers/compliance-main'
  },
  {
    id: DatasetTab.DatasetGroups,
    title: 'Dataset Groups',
    contentComponent: 'datasets/core/containers/dataset-groups',
    lazyRender: true
  },
  /*
   ** Todo : META-9512 datasets - relationships view is unable to handle big payloads
   ** Adding lazy render as a workaround, so as to unblock rest of the tabs on the page.
   */ {
    id: DatasetTab.Relationships,
    title: 'Relationships',
    contentComponent: 'datasets/dataset-relationships',
    lazyRender: true
  },
  {
    id: DatasetTab.Health,
    title: 'Health',
    contentComponent: 'health/entity-detail',
    lazyRender: true
  },
  ...CommonTabProperties
];
