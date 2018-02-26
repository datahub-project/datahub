import Component from '@ember/component';
import { task } from 'ember-concurrency';
import { get, setProperties } from '@ember/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { assert } from '@ember/debug';

export default class UpstreamDatasetContainer extends Component {
  /**
   * urn for the parent dataset
   * @type {string}
   * @memberof UpstreamDatasetContainer
   */
  upstreamUrn: string;

  /**
   * The name of the upstream dataset
   * @type {IDatasetView.nativeName}
   * @memberof UpstreamDatasetContainer
   */
  nativeName: IDatasetView['nativeName'];

  /**
   * A description of the upstream dataset
   * @type {IDatasetView.description}
   * @memberof UpstreamDatasetContainer
   */
  description: IDatasetView['description'];

  constructor() {
    super(...arguments);

    assert('A valid upstreamUrn must be provided on instantiation', typeof this.upstreamUrn === 'string');
  }

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getUpstreamPropertiesTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getUpstreamPropertiesTask').perform();
  }

  /**
   * Task to get properties for the upstream dataset
   * @type {Task<Promise<IDatasetView>>, (a?: {} | undefined) => TaskInstance<Promise<IDatasetView>>>}
   * @memberof UpstreamDatasetContainer
   */
  getUpstreamPropertiesTask = task(function*(this: UpstreamDatasetContainer): IterableIterator<Promise<IDatasetView>> {
    const { nativeName, description }: IDatasetView = yield readDatasetByUrn(get(this, 'upstreamUrn'));
    setProperties(this, { nativeName, description });
  });
}
