import { DatasetTab } from '@datahub/data-models/constants/entity/dataset/tabs';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { fields } from '@datahub/data-models/entity/dataset/fields';

/**
 * Class properties common across instances
 * Dictates how visual ui components should be rendered
 * @readonly
 * @static
 * @type {IEntityRenderProps}
 */
export const getRenderProps = (): IEntityRenderProps => {
  const apiEntityName = 'dataset';
  return {
    apiEntityName,
    search: {
      placeholder: 'Search for datasets...',
      attributes: fields,
      secondaryActionComponents: [],
      customFooterComponents: [
        {
          name: 'top-consumers/insight/top-consumers-insight',
          options: {
            component: 'top-consumers/insight/insight-strip',
            isOptional: true
          }
        },
        { name: 'social/containers/social-metadata' }
      ],
      isEnabled: true
    },
    userEntityOwnership: {
      attributes: fields
    },
    browse: {
      showHierarchySearch: false
    },
    entityPage: {
      route: 'datasets.dataset',
      tabProperties: [],
      defaultTab: DatasetTab.Schema,
      attributePlaceholder: '-',
      apiRouteName: 'datasets',
      pageComponent: {
        name: 'datasets/dataset-page'
      },
      customHeaderComponents: [
        {
          name: 'insight/carousel',
          options: {
            components: [
              {
                name: 'health/carousel-insight',
                options: {
                  priority: 1
                }
              },
              {
                name: 'top-consumers/insight/top-consumers-insight',
                options: {
                  isOptional: true,
                  component: 'top-consumers/insight/insight-card'
                }
              }
            ]
          }
        }
      ]
    }
  };
};
