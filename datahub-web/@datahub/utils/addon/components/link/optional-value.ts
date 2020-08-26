import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/link/optional-value';
import { layout, tagName } from '@ember-decorators/component';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
import { computed } from '@ember/object';

/**
 * Default class for the embedded anchor component
 */
export const defaultAnchorClass = 'optional-value-link';

/**
 * Defines the Link::OptionalValue component, which provides a way to render an anchor element
 * if the supplied value has a `route` attribute and conforms to the IDynamicLinkNode interface
 * However, if this `route` attribute is not present the value is rendered as-is
 * Since the eventual component is an anchor element, this is rendered as a tagless component
 * @export
 * @class LinkOptionalValue
 * @extends {Component}
 * @template M the DynamicLink model type
 * @template R the DynamicLink route type
 * @template QP the query parameters for the DynamicLink
 */
@tagName('')
@layout(template)
export default class LinkOptionalValue<M, R, QP> extends Component {
  /**
   * The DynamicLink component parameters used in generating the properties for the anchor element
   * or the value to be rendered as a string in the DOM
   */
  value: string | IDynamicLinkNode<M, R, QP> = '';

  /**
   * Optionally overridden CSS class selector for the anchor element
   */
  linkClass = defaultAnchorClass;

  /**
   * Will standarize the input of models to an array of strings
   *
   * @readonly
   */
  @computed('value.model')
  get models(): Array<M> {
    const models = typeof this.value !== 'string' ? this.value.model : [];
    const modelsArray = Array.isArray(models) ? models : (models && [models]) || [];
    return modelsArray;
  }
}
