import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/entity-page/entity-header/entity-title';
import { tagName, classNames, layout } from '@ember-decorators/component';

/**
 * Defines the component which displays the type of entity this header is associated with
 * @export
 * @class EntityHeaderEntityType
 * @extends {Component}
 */
@tagName('h4')
@layout(template)
@classNames('wherehows-entity-header-type')
export default class EntityHeaderEntityType extends Component {
  /**
   * The type of the entity
   * @type {string}
   * @memberof EntityHeaderEntityType
   */
  type = 'UNKNOWN';
}
