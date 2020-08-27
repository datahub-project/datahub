import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../../templates/components/entity-page/entity-header/entity-title';
import { set } from '@ember/object';
import { tagName, classNames } from '@ember-decorators/component';

@tagName('h2')
@classNames('wherehows-entity-header-title')
export default class EntityHeaderEntityTitle extends Component {
  layout = layout;

  /**
   * Typically the title or name of the entity
   * @type {string}
   * @memberof EntityHeaderEntityTitle
   */
  title!: string;

  constructor(properties?: object) {
    super(properties);

    // Initialization defaults for expected component attributes
    typeof this.title === 'string' || set(this, 'title', 'UNKNOWN');
  }
}
