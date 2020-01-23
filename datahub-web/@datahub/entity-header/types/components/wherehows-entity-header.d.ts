import Component from '@ember/component';
export default class WhereHowsEntityHeader extends Component {
  layout: any;
  /**
   * @type {string | undefined}
   * @memberof WhereHowsEntityHeader
   */
  entityName: string | undefined;
  /**
   * References the component name to be used to render the title / name of the entity
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityTitleComponent: string;
  /**
   * References the component name used to render a key-value pair of entity attributes
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityPropertyComponent: string;
  /**
   * References the component name used to render an entity attribute / property that
   * deserved prominent placement in the entity header component
   * @type {string}
   * @memberof WhereHowsEntityHeader
   */
  entityAttributePillCalloutComponent: string;
  constructor();
}
