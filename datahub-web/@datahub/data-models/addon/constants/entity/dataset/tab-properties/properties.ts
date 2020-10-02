import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { DatasetTab } from '@datahub/data-models/constants/entity/dataset/tabs';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

/**
 * Configurations for the render props for the nacho table dynamic component
 */
const tableConfigs: INachoTableConfigs<DatasetEntity> = {
  labels: ['label', 'value']
};

/**
 * Properties for the dataset custom properties tab.
 */
export const DatasetPropertiesTab: ITabProperties = {
  id: DatasetTab.Properties,
  title: 'Properties',
  contentComponent: {
    name: 'entity-page/entity-page-content/content-panel',
    options: {
      title: 'Dataset Properties',
      contentComponent: {
        // name of the component
        name: 'entity-page/entity-page-content/nacho-table',
        options: {
          propertyName: 'customDatasetProperties',
          emptyStateMessage: 'We could not find properties for this dataset.',
          tableConfigs
        }
      }
    }
  }
};
