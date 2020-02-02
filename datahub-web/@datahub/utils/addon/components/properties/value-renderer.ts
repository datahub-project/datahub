import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/properties/value-renderer';
import { tagName, layout } from '@ember-decorators/component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';
/**
 * Supported represents the value that this component can render.
 * It can render a string or something unknown in case of using a custom component
 * (using component in field attribute). In that case, the value will be pass through
 * to the custom component.
 */
export type SupportedValue = unknown | string | Array<string>;

/**
 * Renders values using a custom component, also it unwraps arrays
 */
@layout(template)
@tagName('')
export default class PropertiesValueRenderer extends Component {
  /**
   * Dynamic component that will be used to render the value
   */
  component?: IDynamicComponent;

  /**
   * Value that component passed can render
   */
  value: SupportedValue;
}
