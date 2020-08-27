import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../../templates/components/entity-page/entity-header/entity-property';
import { classNames } from '@ember-decorators/component';
import { reads } from '@ember/object/computed';
import { computed } from '@ember/object';
import { typeOf } from '@ember/utils';
import { IEntityHeaderPropertyConfig } from '@datahub/shared/types/entity-page/components/entity-header/entity-property-config';

@classNames('wherehows-entity-header-property')
export default class EntityHeaderEntityProperty extends Component {
  layout = layout;

  /**
   * The name of the entity property / attribute
   * @type {string}
   * @memberof EntityHeaderEntityProperty
   */
  name = '';

  /**
   * The value of the previous entity property
   * @type {string}
   * @memberof EntityHeaderEntityProperty
   */
  value!: string | boolean | Array<string>;

  /**
   * Optional configuration for an entity header property. This will allow us to pass in
   * extra functionality to compute certain values for scenarios where what we want to
   * display is not a simple string
   */
  config?: IEntityHeaderPropertyConfig<EntityHeaderEntityProperty['oneWayValue']>;

  /**
   * One way read from value. It won't allow setting the value back to its original source.
   * This way we can prevent some errors when the source is undefined and trying to set ''
   * to undefined.
   * @type {string}
   * @memberof EntityHeaderEntityProperty
   */
  @reads('value')
  oneWayValue?: string | boolean | Array<string>;

  @computed('value')
  get valueType(): string {
    return typeOf(this.value);
  }

  /**
   * If the configuration for this property wants us to compute a link for the value given,
   * then we do so here.
   * @type {Array<{ ref: string, display: string }> | undefined}
   */
  @computed('oneWayValue', 'config')
  get computedLink(): Array<{ ref: string; display: string }> | undefined {
    const { oneWayValue, config } = this;

    if (!config || !config.computeLink) {
      return;
    }

    const { computeLink } = config;

    if (typeOf(oneWayValue) === 'array') {
      return (oneWayValue as Array<string>).map((item): ReturnType<typeof computeLink> => computeLink(item));
    }

    return [computeLink(oneWayValue)];
  }

  /**
   * Will return a '-' if the value is blank or undefined
   * @type {string}
   * @memberof EntityHeaderEntityProperty
   */
  @computed('oneWayValue')
  get displayValue(): EntityHeaderEntityProperty['value'] | string {
    return this.oneWayValue || '-';
  }
}
