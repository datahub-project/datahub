import Component from '@ember/component';
import { tagName } from '@ember-decorators/component';
import Configurator from 'wherehows-web/services/configurator';
import { unGuardedEntities } from 'wherehows-web/utils/entity/flag-guard';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Main Navigation Bar for Datahub App
 */
@tagName('')
export default class Navbar extends Component {
  /**
   * The list of entities available
   */
  entities: Array<DataModelEntity> = unGuardedEntities(Configurator);

  /**
   * Flag guard to show UMP Datasets I Own
   */
  showUmpFlows: boolean = Configurator.getConfig('showUmpFlows');
}
