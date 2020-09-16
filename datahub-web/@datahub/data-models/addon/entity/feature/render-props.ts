import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { fields } from '@datahub/data-models/entity/feature/fields';
import { FeatureStatusType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-status-type';
import { entityListRenderFields } from '@datahub/data-models/constants/entity/feature/list-fields';
import { sortFields } from '@datahub/data-models/entity/utils/fields';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';
import { FeatureTab, TabProperties } from '@datahub/data-models/constants/entity/feature/tabs';

/**
 * Status Field Common, is the metadata associated for the field Status.
 * Since the meta associated changes depending if you are in browsing section or
 * search, we are keeping this data here instead of '@datahub/data-models/entity/feature/fields'.
 */
const statusFieldCommon: ISearchEntityRenderProps = {
  fieldName: 'status',
  displayName: 'Status',
  desc: '',
  example: '',
  showInAutoCompletion: false,
  showInResultsPreview: false,
  showInFacets: true,
  tagComponent: {
    name: 'features/feature-status'
  }
};
/**
 * Class properties common across instances
 * Dictates how visual ui components should be rendered
 * Implemented as a getter to ensure that reads are idempotent
 */
export const getRenderProps = (): IEntityRenderProps => {
  const secondaryActionComponents = [{ name: 'lists/toggle-on-list' }, { name: 'features/download-config' }];
  return {
    apiEntityName: 'feature',
    search: {
      placeholder: 'Search for Features ...',
      attributes: [
        ...fields,
        {
          ...statusFieldCommon,
          facetDefaultValue: [FeatureStatusType.Published.toLowerCase()]
        }
      ].sort(sortFields),
      isEnabled: true,
      secondaryActionComponents
    },
    userEntityOwnership: {
      attributes: [...fields, statusFieldCommon].sort(sortFields),
      secondaryActionComponents
    },
    browse: {
      showHierarchySearch: true,
      attributes: [
        {
          ...statusFieldCommon,
          // browse requires lowercase values
          facetDefaultValue: [FeatureStatusType.Published.toLowerCase()]
        }
      ]
    },
    entityPage: {
      apiRouteName: 'features',
      route: 'features.feature',
      tabProperties: TabProperties,
      defaultTab: FeatureTab.Metadata,
      attributePlaceholder: '–'
    },
    list: {
      searchResultConfig: {
        attributes: entityListRenderFields,
        isEnabled: true
      }
    }
  };
};
