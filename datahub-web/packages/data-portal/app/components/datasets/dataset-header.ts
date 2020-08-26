import Component from '@ember/component';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { inject as service } from '@ember/service';
import { computed } from '@ember/object';

/**
 * Component for rendering header the on a dataset page
 * @export
 * @class DatasetsDatasetHeader
 * @extends {Component}
 */
export default class DatasetsDatasetHeader extends Component {
  /**
   * Temporary reference to the configurator service used here for feature flag
   */
  @service
  configurator!: IConfigurator;

  /**
   * Guard for Health insight feature
   * @readonly
   */
  @computed('configurator')
  get isHealthInsightEnabled(): boolean {
    return this.configurator.getConfig('useVersion3Health', { useDefault: true, default: false });
  }
}
