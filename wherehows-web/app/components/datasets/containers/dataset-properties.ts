import Component from '@ember/component';
import { get, setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { action } from '@ember-decorators/object';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { updateDatasetDeprecationByUrn } from 'wherehows-web/utils/api/datasets/properties';
import { service } from '@ember-decorators/service';

export default class DatasetPropertiesContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

  /**
   * Flag indicating that the dataset is deprecated
   * @type {IDatasetView.deprecated}
   */
  deprecated: IDatasetView['deprecated'];

  /**
   * Text string, intended to indicate the reason for deprecation
   * @type {IDatasetView.deprecationNote}
   */
  deprecationNote: IDatasetView['deprecationNote'];

  /**
   * Time when the dataset will be decommissioned
   * @type {IDatasetView.decommissionTime}
   */
  decommissionTime: IDatasetView['decommissionTime'];

  /**
   * THe list of properties for the dataset, currently unavailable for v2
   * @type {any[]}
   */
  properties: Array<never> = [];

  /**
   * References the application notifications service
   * @memberof DatasetPropertiesContainer
   * @type {ComputedProperty<Notifications>}
   */
  @service
  notifications: Notifications;

  constructor() {
    super(...arguments);
    this.deprecationNote || (this.deprecationNote = '');
  }

  didInsertElement() {
    get(this, 'getDeprecationPropertiesTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getDeprecationPropertiesTask').perform();
  }

  /**
   * Reads the persisted deprecation properties for the dataset
   * @type {Task<Promise<IDatasetView>, (a?: any) => TaskInstance<Promise<IDatasetView>>>}
   */
  getDeprecationPropertiesTask = task(function*(
    this: DatasetPropertiesContainer
  ): IterableIterator<Promise<IDatasetView>> {
    const { deprecated, deprecationNote, decommissionTime }: IDatasetView = yield readDatasetByUrn(get(this, 'urn'));
    setProperties(this, { deprecated, deprecationNote, decommissionTime });
  });

  /**
   * Persists the changes to the dataset deprecation properties upstream
   * @param {boolean} isDeprecated
   * @param {string} updatedDeprecationNote
   * @param {Date} decommissionTime dataset decommission date
   * @return {Promise<void>}
   */
  @action
  async updateDeprecation(
    this: DatasetPropertiesContainer,
    isDeprecated: boolean,
    updatedDeprecationNote: string,
    decommissionTime: Date | null
  ): Promise<void> {
    const { notify } = get(this, 'notifications');

    try {
      await updateDatasetDeprecationByUrn(
        get(this, 'urn'),
        isDeprecated,
        updatedDeprecationNote || '',
        isDeprecated && decommissionTime ? decommissionTime.getTime() : null
      );

      notify(NotificationEvent.success, {
        content: 'Successfully updated deprecation status'
      });
    } catch (e) {
      notify(NotificationEvent.error, {
        content: `An error occurred: ${e.message}`
      });
    } finally {
      // set current state
      get(this, 'getDeprecationPropertiesTask').perform();
    }
  }
}
