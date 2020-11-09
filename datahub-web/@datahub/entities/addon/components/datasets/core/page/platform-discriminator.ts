import Component from '@glimmer/component';
import { IEntityRenderPropsEntityPage } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';
import { getDatasetUrnParts } from '@datahub/data-models/entity/dataset/utils/urn';

/**
 * Page Component type extracted from render props
 */
type EntityPageComponent = Exclude<IEntityRenderPropsEntityPage['pageComponent'], undefined>;

/**
 * Platform type extracted from dataset
 */
type Platform = Exclude<DatasetEntity['platform'], undefined>;

/**
 * Map between platform and entity page wanted for that specific platform
 */
export type Discriminator = Partial<Record<Platform, EntityPageComponent>>;

/**
 * Arguments for this component
 */
export interface IDatasetsCorePagePlatformDiscriminatorArgs {
  options: {
    // Map with platform and desired page params
    discriminator: Discriminator;
    // If no suitable platform is found, default page will be used
    default: EntityPageComponent;
  };
  // urn of dataset to extract platform
  urn: string;
}

/**
 * This component will allow to map dataset platform with custom page for that platform.
 */
export default class DatasetsCorePagePlatformDiscriminator extends Component<
  IDatasetsCorePagePlatformDiscriminatorArgs
> {
  get component(): IDynamicComponent {
    const { urn, options } = this.args;
    const { discriminator, default: defaultPage } = options;
    const parts = getDatasetUrnParts(urn);
    const discriminatedPage = parts.platform && discriminator[parts.platform];

    if (discriminatedPage) {
      return discriminatedPage;
    }

    return defaultPage;
  }
}
