import Component from '@glimmer/component';
import { computed } from '@ember/object';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { inject as service } from '@ember/service';
import HealthProxy from '@datahub/shared/utils/health/health-proxy';

interface IHealthEntityDetailArgs {
  // The associated Data model instance with properties for the deriving the health score
  entity: DataModelEntityInstance;
  // Optional properties from the render props context where this component is instantiated
  options?: IDynamicComponent['options'];
}

// Top level class for wrapping component
export const baseClass = 'health-entity-detail';

/**
 * Defines the wrapping component for Entity Health metadata, currently rendered in the Health tab
 * @export
 * @class HealthEntityDetail
 * @extends {Component<IHealthEntityDetailArgs>}
 */
export default class HealthEntityDetail extends Component<IHealthEntityDetailArgs> {
  /**
   * Template accessible reference to the base class
   */
  baseClass = baseClass;

  /**
   * Text label or description for the health score
   */
  tooltip = HealthProxy.descriptiveLabel;

  /**
   * Text label or description for the recalculate button
   */
  recalculationTooltip = HealthProxy.descriptiveScoreRecalculationLabel;

  /**
   * Application configurator is used to extract the metadataHealth link from wikiLinks config object
   */
  @service
  configurator!: IConfigurator;

  /**
   * Reference to the Wiki link help resource for health factors
   */
  @computed('configurator')
  get healthFactorsWiki(): string {
    return this.configurator.getConfig('wikiLinks').metadataHealth;
  }

  /**
   * Wiki resource for Health Factors action links
   * @readonly
   */
  @computed('configurator')
  get healthScoresWiki(): string {
    return this.configurator.getConfig('wikiLinks').healthScore;
  }
}
