/**
 * Tab properties for entity profile pages
 * @export
 * @interface ITabProperties
 */
export interface ITabProperties {
  id: string;
  title: string;
  component?: string;
  contentComponent?: string;
  tablistMenuComponent?: string;
  lazyRender?: boolean;
}

/**
 * Lists the tabs available globally to each entity
 * @export
 * @enum {string}
 */
export enum Tab {
  Properties = 'properties',
  Comments = 'comments',
  Schema = 'schema',
  Ownership = 'ownership',
  SampleData = 'sample',
  Relationships = 'relationships',
  Metadata = 'metadata',
  Wiki = 'wiki',
  UserLists = 'userlists',
  UserOwnership = 'userownership'
}

/**
 * A lookup table for each Tabs enumeration
 * @type {Record<Tab, ITabProperties>}
 */
export const TabProperties: Record<Tab, ITabProperties> = {
  [Tab.Schema]: {
    id: Tab.Schema,
    title: 'Schema',
    component: 'datasets/containers/dataset-schema'
  },
  [Tab.Properties]: {
    id: Tab.Properties,
    title: 'Status',
    component: 'datasets/containers/dataset-properties'
  },
  [Tab.Ownership]: {
    id: Tab.Ownership,
    title: 'Ownership',
    component: 'datasets/containers/dataset-ownership'
  },
  [Tab.Comments]: {
    id: Tab.Comments,
    title: 'Comments',
    component: ''
  },
  [Tab.SampleData]: {
    id: Tab.SampleData,
    title: 'Sample Data',
    component: ''
  },
  /*
   ** Todo : META-9512 datasets - relationships view is unable to handle big payloads
   ** Adding lazy render as a workaround, so as to unblock rest of the tabs on the page.
   */
  [Tab.Relationships]: {
    id: Tab.Relationships,
    title: 'Relationships',
    component: 'datasets/dataset-relationships',
    lazyRender: true
  },
  [Tab.Metadata]: {
    id: Tab.Metadata,
    title: 'Metadata',
    component: 'feature-attributes'
  },
  [Tab.Wiki]: {
    id: Tab.Wiki,
    title: 'Docs',
    component: 'institutional-memory/containers/tab',
    lazyRender: true
  },
  [Tab.UserLists]: {
    id: Tab.UserLists,
    title: 'Lists',
    component: '',
    contentComponent: '',
    tablistMenuComponent: 'user/containers/tablist/entity-lists'
  },
  [Tab.UserOwnership]: {
    id: Tab.UserOwnership,
    title: 'Ownership',
    component: '',
    contentComponent: '',
    tablistMenuComponent: 'user/containers/tablist/entity-ownership'
  }
};
