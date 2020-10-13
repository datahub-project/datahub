import { DatasetTab } from '@datahub/data-models/constants/entity/dataset/tabs';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { fields } from '@datahub/data-models/entity/dataset/fields';

/**
 * Aspects for search datasets
 */
const defaultAspects: Array<keyof Com.Linkedin.Metadata.Aspect.DatasetAspect> = [
  'com.linkedin.common.Health',
  'com.linkedin.common.Likes',
  'com.linkedin.common.EntityTopUsage',
  'com.linkedin.common.Status'
  // TODO META-12972: Due to performace reasons, this aspect is disabled until the ticket is resolved
  // 'com.linkedin.common.Follow'
];

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
      isEnabled: true,
      defaultAspects
    },
    userEntityOwnership: {
      attributes: fields,
      defaultAspects
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
          name: 'dynamic-components/entity/field',
          options: { className: 'dataset-header__description', fieldName: 'description' }
        },
        { name: 'datasets/containers/dataset-owner-list' }
      ]
    }
  };
};
