import Component from '@ember/component';
import { assert } from '@ember/debug';
import { CommentTypes } from 'wherehows-web/constants';
import { StringUnionKeyToValue } from 'wherehows-web/typings/generic';
import { DatasetStreamActionsUnion } from 'wherehows-web/constants';
import { noop } from 'wherehows-web/utils/helpers/functions';
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
  handleStreamAction(strategy: DatasetStreamActionsUnion): Promise<boolean> {
    const [, ...args] = [...Array.from(arguments)];

    // assert that handler is in CommentAction needed since we are calling from component template
    // TS currently has no jurisdiction there
    assert(`Expected action to be one of ${Object.keys(StreamActions)}`, strategy in StreamActions);

    return {
      add: (): Promise<boolean> => this.addDatasetComment(...args),
      destroy: (): Promise<boolean> => this.deleteDatasetComment(...args),
      modify: (): Promise<boolean> => this.updateDatasetComment(...args)
    }[strategy]();
  }
}
