import Ember from 'ember';
import { CommentTypes } from 'wherehows-web/constants';
import { StringUnionKeyToValue } from 'wherehows-web/typings/generic';
import { DatasetStreamActionsUnion } from 'wherehows-web/constants';

const { Component, assert } = Ember;

const StreamActions: StringUnionKeyToValue<DatasetStreamActionsUnion> = {
  add: 'add',
  modify: 'modify',
  destroy: 'destroy'
};

export default Component.extend({
  classNames: ['dataset-comments'],

  /**
   * Mapping of available dataset stream action
   * @type {StringUnionKeyToValue<DatasetStreamActionsUnion>}
   */
  streamActions: StreamActions,

  /**
   * Comments on the parent dataset
   * @type Array<IDatasetComment>
   */
  comments: [],

  /**
   * List of available comment types
   * @type ReadonlyArray<CommentTypeUnion>
   */
  commentTypes: CommentTypes,

  actions: {
    /**
     * Handles the action for adding | modifying | destroying a dataset comment
     * invokes handler passed in from parent: controller
     * @return {Promise<boolean>}
     */
    handleStreamAction(strategy: DatasetStreamActionsUnion): Promise<boolean> {
      const [, ...args] = [...Array.from(arguments)];

      // assert that handler is in CommentAction needed since we are calling from component template
      // TS currently has no jurisdiction there
      assert(`Expected action to be one of ${Object.keys(StreamActions)}`, strategy in StreamActions);

      return {
        add: (): Promise<boolean> => this.attrs.addDatasetComment(...args),
        destroy: (): Promise<boolean> => this.attrs.deleteDatasetComment(...args),
        modify: (): Promise<boolean> => this.attrs.updateDatasetComment(...args)
      }[strategy]();
    }
  }
});
