import Component from '@glimmer/component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IPropertiesPanelArgsOptions } from '@datahub/data-models/types/entity/rendering/properties-panel';

/**
 * Arguments for Properties Panel component
 */
export interface IPropertiesPanelArgs {
  // Entity to retrive the properties passed in options
  entity: DataModelEntityInstance;
  // Bucket of options to allow to be rendered as a Dynamic Component
  options: IPropertiesPanelArgsOptions<string>;
}

/**
 * This component will show properties from an entity
 */
export default class PropertiesPanel extends Component<IPropertiesPanelArgs> {}
