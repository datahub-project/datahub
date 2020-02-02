import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/properties/single-value-renderer';
import { tagName, layout } from '@ember-decorators/component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

/**
 * will render a single value with a custom component (or not)
 *
 * It helps using the same component for different entities
 */
@layout(template)
@tagName('')
export default class PropertiesSingleValueRenderer extends Component {
  /**
   * Dynamic component that will be used to render the value
   */
  component?: IDynamicComponent;

  /**
   * Value that component passed can render,
   * if no component, then it will render the string
   */
  value: unknown | string;
}
