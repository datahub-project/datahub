import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

/**
 * Optional configuration for an entity header property. This will allow us to pass in
 * extra functionality to compute certain values for scenarios where what we want to
 * display is not a simple string
 */
export interface IEntityHeaderPropertyConfig<T> {
  // whether the property to render is a value (string) or a link
  displayType: 'value' | 'link' | 'component';
  // if displayType is 'link', then 'computeLink' must be provided
  computeLink?: (value: T) => { ref: string; display: string };
  // if displayType is 'component, then 'componentOptions' must be provided
  componentOptions?: IDynamicComponent;
}
