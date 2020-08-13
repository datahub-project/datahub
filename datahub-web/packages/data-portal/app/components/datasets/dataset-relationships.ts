import Component from '@ember/component';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { inject as service } from '@ember/service';

export default class DatasetRelationships extends Component {
  /**
   * Will be needed to fetch flag guards
   */
  @service
  configurator: IConfigurator;

  /**
   * Flag to determine if we have to show lineage graph v3
   */
  showLineageV3 = this.configurator.getConfig('showLineageV3', { useDefault: true, default: false });
}
