import { Tab } from '@datahub/data-models/constants/entity/shared/tabs';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { fields } from '@datahub/data-models/entity/dataset/fields';
import { getTabPropertiesFor } from '@datahub/data-models/entity/utils';

/**
 * Class properties common across instances
 * Dictates how visual ui components should be rendered
 * @readonly
 * @static
 * @type {IEntityRenderProps}
 */
export const getRenderProps = (): IEntityRenderProps => {
  // TODO: right now logic is in dataset-main.ts computed property datasetTabs
  const tabIds: Array<Tab> = [];

  return {
    search: {
      placeholder: 'Search for datasets...',
      attributes: fields,
      apiName: 'dataset'
    },
    browse: {
      // TODO META-8863 remove once dataset is migrated
      showCount: true,
      showHierarchySearch: false,
      entityRoute: 'datasets.dataset'
    },
    entityPage: {
      tabIds,
      route: 'datasets.dataset',
      tabProperties: getTabPropertiesFor(tabIds),
      defaultTab: Tab.Schema,
      attributePlaceholder: '-'
    }
  };
};
