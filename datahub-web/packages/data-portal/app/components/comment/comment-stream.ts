import Component from '@ember/component';
import { assert } from '@ember/debug';
import { StreamCommentActionsUnion } from 'wherehows-web/constants';
import { StringUnionKeyToValue } from 'wherehows-web/typings/generic';
import { noop } from 'wherehows-web/utils/helpers/functions';

/**
 * Actions available for comment stream
 * @type {CommentActions}
 */
const CommentActions: StringUnionKeyToValue<StreamCommentActionsUnion> = {
  create: 'create',
  update: 'update',
  destroy: 'destroy'
};

export default Component.extend({
  tagName: 'ul',

  classNames: ['comment-stream'],

  /**
   * Mapping of available comment action
   * @type {StringUnionKeyToValue<StreamCommentActionsUnion>}
   */
  commentActions: CommentActions,

  /**
   * Default no-op function to add a comment
   * @type {Function}
   */
  addCommentToStream: noop,

  /**
   * Default no-op function to delete a comment
   * @type {Function}
   */
  deleteCommentFromStream: noop,

  /**
   * Default no-op function to update a comment
   * @type {Function}
   */
  updateCommentInStream: noop,

  actions: {
    /**
     * Async handles CrUD operations for comment stream actions, proxies to parent closure actions
     * @param {StreamCommentActionsUnion} strategy
     * @return {Promise<boolean>}
     */
    handleStreamComment(strategy: StreamCommentActionsUnion): Promise<boolean> {
      const [, ...args] = arguments;

      // assert that handler is in CommentAction needed since we are calling from component template
      // TS currently has no jurisdiction there
      assert(`Expected action to be one of ${Object.keys(CommentActions)}`, strategy in CommentActions);

      return {
        create: (): Promise<boolean> => this.addCommentToStream(...args),
        destroy: (): Promise<boolean> => this.deleteCommentFromStream(...args),
        update: (): Promise<boolean> => this.updateCommentInStream(...args)
      }[strategy]();
    }
  }
});
