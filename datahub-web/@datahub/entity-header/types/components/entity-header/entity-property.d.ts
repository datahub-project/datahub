import Component from '@ember/component';

/**
 * Optional configuration for an entity header property. This will allow us to pass in
 * extra functionality to compute certain values for scenarios where what we want to
 * display is not a simple string
 */
export interface IEntityHeaderPropertyConfig<T> {
  displayType: 'value' | 'link';
  computeLink?: (value: T) => { ref: string; display: string };
}

export default class EntityHeaderEntityProperty extends Component {
  layout: any;
  /**
   * The name of the entity property / attribute
   * @type {string}
   * @memberof EntityHeaderEntityProperty
   */
  name: string;
  /**
   * The value of the previous entity property
   * @type {string}
   * @memberof EntityHeaderEntityProperty
   */
  value: string;
  /**
   * Creates an instance of EntityHeaderEntityProperty.
   * @memberof EntityHeaderEntityProperty
   */
  constructor();
}
