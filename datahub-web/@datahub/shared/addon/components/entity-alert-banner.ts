import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/entity-alert-banner';
import { layout, classNames } from '@ember-decorators/component';

export const baseClass = 'entity-alert-banner';

/**
 * A banner that displays entity level alerts
 *
 * TODO META-11234: Refactor entity alert banner to be generic
 * The banner template is currently hardcoded and does not use
 */
@classNames(`${baseClass}-container`)
@layout(template)
export default class EntityAlertBanner extends Component {
  /**
   * Declared for convenient access in the template
   */
  baseClass = baseClass;

  /**
   * The subject of the alert message
   */
  subject = '';

  /**
   * The body of the alert message that provides additional detail
   */
  body = '';
}
