import Component from '@ember/component';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { inject as service } from '@ember/service';
import Notifications from '@datahub/utils/services/notifications';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasetSnapshot } from 'wherehows-web/utils/api/datasets/dataset';
import { set } from '@ember/object';
import { IDatasetSnapshot } from '@datahub/metadata-types/types/metadata/dataset-snapshot';
import { tagName } from '@ember-decorators/component';
import { task } from 'ember-concurrency';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

/**
 * Retrieves snapshot data for a specific ump dataset if the platform meets the condition of being a ump dataset
 * @export
 * @class DatasetSnapshotContainer
 * @extends {Component}
 */
@tagName('')
@containerDataSource('getContainerDataTask', ['dataset'])
export default class DatasetSnapshotContainer extends Component {
  /**
   * Reference to the application notifications Service
   * @type {Notifications}
   * @memberof DatasetSnapshotContainer
   */
  @service
  notifications: Notifications;

  /**
   * References a specific dataset entity that could potentially be a UMP dataset
   * @type {IDatasetView}
   * @memberof DatasetSnapshotContainer
   */
  dataset: IDatasetView;

  /**
   * Snapshot data for this.dataset if the dataset is a UMP dataset, otherwise undefined value
   * @type {IDatasetSnapshot}
   * @memberof DatasetSnapshotContainer
   */
  snapshot?: IDatasetSnapshot;

  /**
   * Container task will fetch and set snapshot data for this.dataset
   * @memberof DatasetSnapshotContainer
   */
  @(task(function*(this: DatasetSnapshotContainer): IterableIterator<Promise<IDatasetSnapshot>> {
    const { notify } = this.notifications;
    const { uri, platform } = this.dataset;

    if (platform === DatasetPlatform.UMP) {
      try {
        const datasetSnapshot: IDatasetSnapshot = yield readDatasetSnapshot(uri);
        set(this, 'snapshot', datasetSnapshot);
      } catch (e) {
        notify({ type: NotificationEvent.error, content: e });
        throw e;
      }
    }
  }).restartable())
  getContainerDataTask!: ETaskPromise<IDatasetSnapshot>;
}
