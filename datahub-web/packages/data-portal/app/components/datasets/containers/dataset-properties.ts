import Component from '@ember/component';
import { get, setProperties } from '@ember/object';
import { Task, task } from 'ember-concurrency';
import { action } from '@ember/object';
import Notifications from '@datahub/utils/services/notifications';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { updateDatasetDeprecationByUrn } from 'wherehows-web/utils/api/datasets/properties';
import { inject as service } from '@ember/service';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { NotificationEvent } from '@datahub/utils/constants/notifications';

@containerDataSource('getDeprecationPropertiesTask', ['urn'])
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
  deprecationNote: IDatasetView['deprecationNote'] = '';

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

  /**
   * Reads the persisted deprecation properties for the dataset
   * @type {Task<Promise<IDatasetView>, (a?: any) => TaskInstance<Promise<IDatasetView>>>}
   */
  @task(function*(this: DatasetPropertiesContainer): IterableIterator<Promise<IDatasetView>> {
    const { deprecated, deprecationNote, decommissionTime }: IDatasetView = yield readDatasetByUrn(this.urn);
    setProperties(this, { deprecated, deprecationNote, decommissionTime });
  })
  getDeprecationPropertiesTask!: Task<Promise<IDatasetView>, () => Promise<IDatasetView>>;
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
    decommissionTime: number | null
  ): Promise<void> {
    const { notify } = get(this, 'notifications');

    try {
      await updateDatasetDeprecationByUrn(
        get(this, 'urn'),
        isDeprecated,
        updatedDeprecationNote || '',
        isDeprecated && decommissionTime ? decommissionTime : null
      );

      notify({
        type: NotificationEvent.success,
        content: 'Successfully updated deprecation status'
      });
    } catch (e) {
      notify({
        type: NotificationEvent.error,
        content: `An error occurred: ${e.message}`
      });
    } finally {
      // set current state

      this.getDeprecationPropertiesTask.perform();
    }
  }
}
