import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/entity-page/wherehows-entity-header';
import { tagName, classNames } from '@ember-decorators/component';
import { set } from '@ember/object';

@tagName('section')
@classNames('wherehows-entity-header')
export default class WhereHowsEntityHeader extends Component {
  layout = layout;

  /**
   * References the component name to be used to render the title / name of the entity
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityTitleComponent!: string;

  /**
   * References the component name used to render a key-value pair of entity attributes
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityPropertyComponent!: string;

  /**
   * References the component name used to render an entity attribute / property that
   * deserved prominent placement in the entity header component
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityAttributePillCalloutComponent!: string;

  /**
   * References the component name used to render the entity type
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityTypeComponent!: string;

  constructor(properties?: object) {
    super(properties);

    // Initialization defaults for expected component attributes
    typeof this.entityTitleComponent === 'string' ||
      set(this, 'entityTitleComponent', 'entity-page/entity-header/entity-title');
    typeof this.entityPropertyComponent === 'string' ||
      set(this, 'entityPropertyComponent', 'entity-page/entity-header/entity-property');
    typeof this.entityAttributePillCalloutComponent === 'string' ||
      set(this, 'entityAttributePillCalloutComponent', 'entity-page/entity-header/attribute-callout');
    typeof this.entityTypeComponent === 'string' ||
      set(this, 'entityTypeComponent', 'entity-page/entity-header/entity-type');
  }
}
