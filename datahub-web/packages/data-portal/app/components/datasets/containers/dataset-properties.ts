import Component from '@ember/component';
import { get } from '@ember/object';
import { action } from '@ember/object';
import Notifications from '@datahub/utils/services/notifications';
import { updateDatasetDeprecationByUrn } from 'datahub-web/utils/api/datasets/properties';
import { inject as service } from '@ember/service';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { alias } from '@ember/object/computed';

type DeprecationType = Exclude<Com.Linkedin.Dataset.Dataset['deprecation'], undefined>;

export default class DatasetPropertiesContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

  /**
   * DatasetEntity instace passed down from the entity-page container
   */
  entity: DatasetEntity;

  /**
   * Flag indicating that the dataset is deprecated
   */
  @alias('entity.deprecated')
  deprecated: DeprecationType['deprecated'] | undefined;

  /**
   * Text string, intended to indicate the reason for deprecation
   */
  @alias('entity.deprecationNote')
  deprecationNote: DeprecationType['note'] | undefined = '';

  /**
   * Time when the dataset will be decommissioned
   */
  @alias('entity.decommissionTime')
  decommissionTime: DeprecationType['decommissionTime'] | undefined;

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
    }
  }
}
