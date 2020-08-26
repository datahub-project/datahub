import Component from '@ember/component';
import { CommentTypes } from 'datahub-web/constants';
import { StringUnionKeyToValue } from 'datahub-web/typings/generic';
import { DatasetStreamActionsUnion } from 'datahub-web/constants';
import { noop } from 'lodash';
import { classNames } from '@ember-decorators/component';
import { action } from '@ember/object';

const StreamActions: StringUnionKeyToValue<DatasetStreamActionsUnion> = {
  add: 'add',
  modify: 'modify',
  destroy: 'destroy'
};

@classNames('dataset-comments')
export default class DatasetComments extends Component {
  /**
   * Mapping of available dataset stream action
   * @type {StringUnionKeyToValue<DatasetStreamActionsUnion>}
   */
  streamActions = StreamActions;

  /**
   * Comments on the parent dataset
   * @type Array<IDatasetComment>
   */
  comments = [];

  /**
   * List of available comment types
   * @type ReadonlyArray<CommentTypeUnion>
   */
  commentTypes = CommentTypes;

  /**
   * Default no-op function to add a dataset comment
   * @type {Function}
   */
  addDatasetComment = noop;

  /**
   * Default no-op function to delete a dataset comment
   * @type {Function}
   */
  deleteDatasetComment = noop;

  /**
   * Default no-op function to update a dataset comment
   * @type {Function}
   */
  updateDatasetComment = noop;

  /**
   * Handles the action for adding | modifying | destroying a dataset comment
   * invokes handler passed in from parent: controller
   * @return {Promise<boolean>}
   */
  @action
  handleStreamAction(strategy: DatasetStreamActionsUnion, ...args: Array<unknown>): void {
    return {
      add: (): void => this.addDatasetComment(...args),
      destroy: (): void => this.deleteDatasetComment(...args),
      modify: (): void => this.updateDatasetComment(...args)
    }[strategy]();
  }
}
