import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

/**
 * Properties Panel accepted options
 */
export interface IPropertiesPanelArgsOptions<ValidPropertiesName> {
  // List of properties to display
  properties: Array<IStandardDynamicProperty<ValidPropertiesName>>;
  // Number of column to display the properties
  columnNumber: number;
  // Whether this component it is embeded or it should show its own borders and margins
  standalone: boolean;
}

/**
 * Properties panel label accepted options
 */
export interface IPropertyPanelLabelComponentOptions {
  // Display name of the property to show in the UI
  displayName: string;
  // Optional text providing additional information about the property rendered as a tooltip
  tooltipText?: string;
}

/**
 * The property panel label component properties for dynamic rendering
 */
export interface IPropertyPanelLabelComponent {
  // Name of the component to render
  name: string;
  // The options permissible for a panel label component
  options?: IPropertyPanelLabelComponentOptions;
}

/**
 * Standarized dynamic property across Datahub
 */
export interface IStandardDynamicProperty<ValidPropertiesName> {
  // Name of the property to retreive the value
  name: ValidPropertiesName;
  // Display name of the property to show in the UI
  displayName?: string;
  // Dynamic component used to render the label for this property
  labelComponent?: IPropertyPanelLabelComponent;
  // Dynamic component that this property can use to be rendered
  component?: IDynamicComponent;
}
