import Component from '@glimmer/component';
import { IPropertyPanelLabelComponentOptions } from '@datahub/data-models/types/entity/rendering/properties-panel';

interface IEntityPropertiesPanelLabelArgs {
  // Options provided as rendering logic for the component
  options: IPropertyPanelLabelComponentOptions;
}

/**
 * The generic component for property panel labels on the entity page
 */
export default class EntityPropertiesPanelLabel extends Component<IEntityPropertiesPanelLabelArgs> {}
