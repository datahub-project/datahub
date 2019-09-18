import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { IDatasetApiView } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

/**
 * Fields for dataset
 */
export const fields: Array<ISearchEntityRenderProps> = [
  {
    showInAutoCompletion: true,
    fieldName: 'origin',
    showInResultsPreview: true,
    displayName: 'Origin',
    showInFacets: true,
    desc: 'The origin of the dataset',
    example: 'origin:PROD'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'name',
    showInResultsPreview: false,
    displayName: 'name',
    showInFacets: false,
    desc: 'The name of the dataset',
    example: 'name:TRACKING.PageViewEvent',
    compute(dataset: IDatasetApiView): string {
      const name = dataset.nativeName;
      const platform: DatasetPlatform = dataset.platform;
      // UMP datasets have been defined as <bucket>.<datasetName> format, so we want to extract out only
      // the name part to display
      if (platform === DatasetPlatform.UMP) {
        return name.split('.')[1] || name;
      }

      return name;
    }
  },
  {
    showInAutoCompletion: true,
    fieldName: 'owners',
    showInResultsPreview: true,
    displayName: 'owners',
    showInFacets: false,
    desc: 'The confirmed owners for the dataset',
    example: 'owners:jweiner'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'platform',
    showInResultsPreview: true,
    displayName: 'Platform',
    showInFacets: true,
    desc: 'The platform of the dataset',
    example: 'platform:kafka'
  }
];
