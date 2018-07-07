import Component from '@ember/component';
import { get, set } from '@ember/object';
import { task } from 'ember-concurrency';
import { DatasetPlatform } from 'wherehows-web/constants';
import { readDatasetsCount } from 'wherehows-web/utils/api/datasets/dataset';
import { IDynamicLinkNode } from 'wherehows-web/typings/app/datasets/dynamic-link';

export default class DataPlatformContainer extends Component {
  /**
   * References the dynamic link properties for the related platform
   * @type {IDynamicLinkNode}
   */
  platformParams: IDynamicLinkNode;

  /**
   * Props the dataset platform, including name and count of datasets within the platform
   * @type {{platform: DatasetPlatform | string, count?: number}}
   */
  platformProps: { platform: DatasetPlatform | string; count: number };

  constructor() {
    super(...arguments);
    this.platformProps = { platform: get(this, 'platformParams').title, count: 0 };
  }

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getDataPlatformTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getDataPlatformTask').perform();
  }

  /**
   * Task to request the data platform's count
   * @type {(Task<Promise<number>, (a?: any) => TaskInstance<Promise<number>>>)}
   */
  getDataPlatformTask = task(function*(this: DataPlatformContainer): IterableIterator<Promise<number>> {
    const { title: platform } = get(this, 'platformParams');
    const count = yield readDatasetsCount({ platform });

    set(this, 'platformProps', { platform, count });
  });
}
