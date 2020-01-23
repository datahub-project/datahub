import Component from '@ember/component';
import { tagName } from '@ember-decorators/component';
import { getConfig } from 'wherehows-web/services/configurator';
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
  entities: Array<DataModelEntity> = unGuardedEntities(getConfig);
}
