import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * Lists the Feature Tabs available
 */
export enum FeatureTab {
  Metadata = 'metadata',
  Statistics = 'statistics'
}

/**
 * Optional tab that will be added during runtime
 */
export const FeatureStatisticTab = {
  id: FeatureTab.Statistics,
  title: 'Statistics',
  contentComponent: 'features/feature-statistics'
};

/**
 * List of default tabs for feature
 */
export const TabProperties: Array<ITabProperties> = [
  {
    id: FeatureTab.Metadata,
    title: 'Metadata',
    contentComponent: 'features/feature-attributes'
  }
];
